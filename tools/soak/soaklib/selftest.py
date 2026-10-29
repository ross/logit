"""`soak.py self-test`: the driver's pure parts against embedded fixtures, plus `validate()` over
every shipped scenario. `run` calls it first, so a broken reducer or rule can't score a run.

It covers the duration parser, every `validate()` rejection rule, `expand()`'s cycle
repetition, the NDJSON reducers (counters per life split where uptime decreases, gauge series),
the stderr classifier (a raw `thread '...' panicked at` line included), the slope function, and
the checks against small synthetic run directories: a failed fault action, an aborted run and its
exit code, an fd sample inside a later fault, a steady state too thin to judge, and
`rss_settle`: its rules, a stepped recovery inside it, a whole-run leak FAILing through it, and
coverage WARNing where it excuses everything. For the
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
A whole `execute()` against a Docker stand-in, on a fake clock that jumps past `STALL_S` inside
a fault, stops the schedule, reverts the open fault early, runs the end sequence, and exits
`STALLED_EXIT`, as `check` does on its directory; `run_retrying` reruns only that status, with
the same arguments.

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
naming none. On an HTTP wire it covers the generator sink's kind checked at load either way,
the fault predicates and judgments, `prometheus_in`'s wire keyed on its mode, and runs into
`influxdb_in`, `otlp_in`, `splunk_hec_in`, `datadog_in`, and `prometheus_in`: G from the
writer's acknowledged events, a stop judged and a kill reported, a judged window's surplus
against its transport-error attempts and its ack timeouts and not its `5xx` answers or failed
`/ack` polls, a `closed_consumer` refusal taken off W, each listener's request and deferral
identities in `ledger.intake` broken, `prometheus_in`'s with no route tag, the end-to-end bound
in `ledger.summary`, and a listener kind the checks can't read.

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
and `drops` with cued and uncued refusals and drops. For the `influxdb` shape, it covers a
body `influxdb_out` sent and hand-built escape and grammar cases (`standin/influxdb-vectors.json`)
through the line-protocol parser, the bodies, precisions, encodings, and label clashes refused,
V as one point per series and timestamp (a rewrite naming both requests, a changed value a
conflict), the `204` with no length and the query split off the path on loopback ports, the
class rules, and the checks over an influxdb stand-in run: a rewrite reported, and FAILing
after a 2xx or under `at_most_once`; a changed resend; a rejected step's points equal to the
gap, with an unexplained gap and a stored rejected point each FAILing; and two destinations
FAILing.

For a rolling overlap, it covers every `[overlap]` rule (the table's own, against the ledger and
each instance's config, the steps on the holder and on the instances, the role map and each
instance's network mode, and list-valued generator keys), the fault predicates with a holder,
the driver (`compose.env`, the network modes compose reports, readiness through the container's
own `LOGIT_ADMIN`, and the end sequence's one wait before each instance stops), and a
two-instance run: a clean swap passing every row, each instance judged on its own lives and
labeled series, a loss at the close within its bound passing and one past it FAILing, a kill
reported and not judged, a series with no instance's label FAILing `ledger.egress`, and a stall
of the one instance running FAILing `progress`.

For `signal` and `rename` steps, it covers every validation rule (the keys each takes, `HUP`
only, on a service that runs `logit`, never inside that service's other steps, a rename's file a
`stdio_out` target in `/shared` of its service's config with a SIGHUP starting inside it, once
per run, and neither in a `[random]` schedule), the actions through a fake Docker and a
temporary directory, that neither affects any leg, `prepare()` making the shared directory for a
rename, and `file.reopen` over a synthetic run of two renames: both rows passing, a late line in
the renamed file, an early line in the new one, a count off by one, and a missing new file each
FAILing, a kill's count reported, neither step opening a fault window, a SIGHUP's info line
leaving `self_log` alone, an `[[expect]]` through a signal step, and deleted files read
from the run's recorded counts.

For `[tls]`, `rotate`, and `probe`, it covers every validation rule (the table's keys, names,
DNS names, and usages; the keys each step takes; a rotate's certificate read by its service's
config, outside a comment, its days rising, once per run; a probe of a server certificate inside
a rotate on its own service after a SIGHUP, never inside a lifecycle fault; neither in a
`[random]` schedule), the pinned certs.sh command lines through a fake Docker, `prepare()`
making the CA and each first issue and stopping on a failed one, and over synthetic runs:
`tls.served` reading the generation in force through a broken rotation, a wrong serial, an
unverified chain, a missing first issue, and a failed probe each FAILing; `self_log` excusing
`tls_reload_failed` inside a broken rotation on its service, and inside another rotation on a
polling service when a reload follows it there, a torn read, but not with no later reload or on a
service that doesn't poll; a rotation a later probe reads loaded by a SIGHUP or a start on a SUT
that doesn't poll; the TLS directory's mode under a narrow umask; and the `rise` reducer.

For `sample` and `dedup_timestamps` in the path, it covers sampling.py's XXH64 against xxHash's
published vectors and `logit_core::sampling`'s pinned ones, and the shipped sample-path
scenario's selectors against its verdicts; every `vm_absent_selector` and `merge` rule; and over
synthetic runs, `ledger.absent` passing an empty export and FAILing a stored series, a failed
export, and one that never ran, and the merge term crediting M back in the final and a stopped
life, with discards past M, a superseded reading in either life, and an undeclared merge each
FAILing.

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
import struct
import subprocess
import sys
import tempfile
import time
import tomllib
import zlib
from datetime import datetime, timezone
from pathlib import Path

from . import (checks, collect, docker, driver, faults, otlp, report, sampling, scenario, standin,
               telemetry,
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
    # `max_duration` refuses a --duration past it, and a scenario whose own duration is.
    capped = mutate(lambda r: r.update(max_duration="20m"))
    _refused(path, capped, "duration 20m1s is longer than the scenario's max_duration 20m",
             "a --duration past max_duration", duration=1201)
    _refused(path, mutate(lambda r: r.update(max_duration="10m")),
             "duration 16m is longer than the scenario's max_duration 10m",
             "a scenario's own duration past its max_duration")
    _refused(path, mutate(lambda r: r.update(max_duration="soon")),
             "scenario.max_duration: not a duration", "a max_duration that isn't a duration")
    try:
        loaded = scenario.from_dict(copy.deepcopy(capped), path)
        scenario.validate(loaded, duration=1200)
        expect(loaded.max_duration == 1200, "max_duration loads as seconds")
    except scenario.ScenarioError as err:
        expect(False, f"a --duration at max_duration is refused: {err.problems}")
    tail = scenario.load(root / "scenarios" / "tail-rotate" / "scenario.toml")
    expect(tail.max_duration is not None and tail.max_duration >= tail.duration,
           f"tail-rotate caps --duration at its retention and fd_growth, got "
           f"{tail.max_duration}")
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


def _stalled(root):
    """A tick that runs far late, as across a host suspend, records a `stalled` phase, and
    `run` FAILs it; a slow action's lateness, under `STALL_S`, doesn't."""
    loaded = scenario.load(root / "scenarios" / "statsd-vm" / "scenario.toml")

    def loop(jump):
        run = driver.Run(root, loaded, 5.0, None, False, "/nonexistent", [], "logit:soak",
                         roles=scenario.BASE_ROLES)
        run.steps = []
        run.t0 = 1000.0
        run.timeline = _Records()
        clock = [1000.0]
        for name in ("poll_ready", "inspect_all", "sample_stats", "sample_freshness"):
            setattr(run, name, lambda: None)
        reals = (driver._now, driver.time.sleep, driver._print)
        driver._now = lambda: clock[0]

        def sleep(seconds):
            clock[0] += seconds + (jump if clock[0] == 1002.0 else 0)

        driver.time.sleep = sleep
        driver._print = lambda text, stream=None: None
        try:
            run.loop()
        finally:
            driver._now, driver.time.sleep, driver._print = reals
        return [r for r in run.timeline if r.get("phase") == "stalled"]

    stalled = loop(8000.0)
    expect(len(stalled) == 1 and stalled[0]["late_s"] == 8000.0 and stalled[0]["since"] == 3,
           f"a tick 8,000 s late records a stalled phase, got {stalled}")
    expect(loop(driver.STALL_S - 1) == [],
           f"a tick under STALL_S late records none, got {loop(driver.STALL_S - 1)}")
    with tempfile.TemporaryDirectory() as tmp:
        run_dir = _run_dir(tmp, "stalled", [_phase("start", 0, t0=T0),
                                            _phase("stalled", 1183, late_s=8445.6, since=1182),
                                            _phase("end_begin", 9628), _phase("end_end", 9730)])
        result = _status(run_dir, checks.check_run)
        expect(result.status == checks.FAIL
               and "stalled: the driver's tick ran 8,446s late from +1182s" in result.detail,
               f"run FAILs a stalled driver, got {result}")
        polls = [{"t": T0 + offset, "offset": offset, "svc": "logit"}
                 for offset in list(range(0, 1121, 5)) + [9628]]
        old = _run_dir(tmp, "stalled-old", [_phase("start", 0, t0=T0),
                                            _phase("end_begin", 9628), _phase("end_end", 9730)])
        (old / "watchdog.jsonl").write_text("".join(json.dumps(r) + "\n" for r in polls))
        result = _status(old, checks.check_run)
        expect(result.status == checks.FAIL
               and "stalled: the driver's tick ran 8,508s late from +1120s" in result.detail,
               f"run reads a stall from the watchdog's gap in an older run, got {result}")
        # A driver that records stalls is read by its phase alone: one slow iteration leaves a
        # watchdog gap and no phase, and isn't a stall.
        new = _run_dir(tmp, "stalled-new", [_phase("start", 0, t0=T0, marks_stalls=True),
                                            _phase("end_begin", 9628), _phase("end_end", 9730)])
        (new / "watchdog.jsonl").write_text("".join(json.dumps(r) + "\n" for r in polls))
        result = _status(new, checks.check_run)
        expect(result.status == checks.PASS,
               f"a run whose driver records stalls is read by the phase alone, got {result}")


class _AnswerDocker:
    """Answers every call `execute()` makes with success and records each command: every
    container running since its first start, and no daemon."""

    def __init__(self):
        self.calls = []

    def _ok(self, args):
        self.calls.append(list(args))
        return docker.Result(0, "", "")

    def project_containers(self):
        return []

    def run(self, args, timeout=None, stdout=None, stderr=None):
        return self._ok(args)

    def compose(self, *args, timeout=None):
        return self._ok(["compose", *args])

    def run_oneshot(self, args, timeout=None):
        return self._ok(["run", "--rm", *args])

    def exec_(self, container, argv, timeout=None):
        return self._ok(["exec", container, *argv])

    def logs_tail(self, container, lines, timeout=None):
        return self._ok(["logs", "--tail", str(lines), container])

    def logs_to(self, container, stdout_path, stderr_path, since=None, until=None,
                timeout=None):
        Path(stdout_path).write_text("")
        Path(stderr_path).write_text("")
        return self._ok(["logs", container])

    def container_id(self, service):
        return f"id-{service}"

    def port(self, service, private_port):
        return "127.0.0.1:1"

    def inspect(self, container, timeout=None):
        return {"State": {"Status": "running", "StartedAt": "first", "ExitCode": 0}}

    def stats(self, containers, timeout=None):
        return []


def _stall_abort(root):
    """A run whose clock jumps more than `STALL_S` while a fault is open stops the schedule at
    once: no step applies after the stall, the end sequence reverts the open one early and runs
    to `end_end`, `results.md` names the abort, and both `run` and a re-`check` exit
    `STALLED_EXIT`."""
    loaded = scenario.load(root / "scenarios" / "statsd-vm" / "scenario.toml")
    with tempfile.TemporaryDirectory() as tmp:
        run = driver.Run(root, loaded, loaded.duration, None, False, Path(tmp) / "out", [],
                         "logit:soak", roles=scenario.BASE_ROLES)
        run.prepare()
        fake = _AnswerDocker()
        run.docker = run.ctx.docker = run.capture.docker = fake
        clock = [1000.0]
        # c0s1, netem on logit, is open from +60 s to +180 s, and c0s2 is due at +240 s. The
        # host sleeps at +90 s for 200 s, so both are overdue when the driver wakes.
        jumped = []

        def sleep(seconds):
            clock[0] += seconds
            if not jumped and clock[0] >= 1090.0:
                jumped.append(clock[0])
                clock[0] += 200.0

        reals = (driver._now, driver.time.sleep, driver._print, vm._get)
        driver._now = lambda: clock[0]
        driver.time.sleep = sleep
        driver._print = lambda text, stream=None: None
        vm._get = lambda base, path, params=None, timeout=None: (
            "OK" if path == "/health" else "")
        try:
            code = run.execute()
        finally:
            driver._now, driver.time.sleep, driver._print, vm._get = reals
        timeline = collect.read_jsonl(run.run_dir / "timeline.jsonl")
        phases = [r["phase"] for r in timeline if r.get("event") == "phase"]
        stall = next((r for r in timeline if r.get("phase") == "stalled"), None)
        expect(stall is not None and phases.count("stalled") == 1
               and phases[phases.index("stalled"):phases.index("stalled") + 3]
               == ["stalled", "aborted", "end_begin"]
               and "end_end" in phases and "down" in phases,
               f"a stall records stalled and aborted, then runs the end sequence to end_end, "
               f"got {phases}")
        late = [r for r in timeline if r.get("event") in ("apply", "revert")
                and stall is not None and r["started_at"] >= stall["t"]]
        expect([(r["event"], r["step"], bool(r.get("early"))) for r in late]
               == [("revert", "c0s1", True)],
               f"after a stall nothing applies, and the open step reverts early, got "
               f"{[(r['event'], r['step'], r.get('early')) for r in late]}")
        ended = next((r for r in timeline if r.get("phase") == "end_end"), None)
        expect(stall is not None and ended is not None and ended["t"] - stall["t"] < 600,
               f"the end sequence after a stall finishes inside 10 minutes, got "
               f"{None if ended is None or stall is None else ended['t'] - stall['t']}")
        expect(code == driver.STALLED_EXIT,
               f"run exits STALLED_EXIT after a stall, got {code}")
        text = (run.run_dir / "results.md").read_text()
        expect(checks.STALL_ABORT_REASON in text and "stalled: the driver's tick ran 20" in text,
               "results.md names the stall and why the run ended early")
        rescored = driver.exit_code(checks.run_all(run.run_dir))
        expect(rescored == driver.STALLED_EXIT,
               f"check exits STALLED_EXIT on a stalled run directory, got {rescored}")


def _retry_stalled():
    """`run_retrying` reruns a stalled run with the same arguments, waiting between attempts,
    and stops at the first other status or after its retries."""

    def attempt(codes, retries):
        calls, waits = [], []
        reals = (driver.run, driver.time.sleep, driver._print)

        def fake_run(*args):
            calls.append(args)
            return codes[len(calls) - 1]

        driver.run = fake_run
        driver.time.sleep = waits.append
        driver._print = lambda text, stream=None: None
        try:
            code = driver.run_retrying(retries, "root", "path", 600.0, 7, False, "out", [],
                                       "image", "env")
        finally:
            driver.run, driver.time.sleep, driver._print = reals
        return code, calls, waits

    stalled = driver.STALLED_EXIT
    code, calls, waits = attempt([stalled, 0], 2)
    expect(code == 0 and len(calls) == 2 and calls[0] == calls[1]
           and waits == [driver.RETRY_WAIT_S],
           f"a stalled run reruns with the same duration and seed, got {code} {calls} {waits}")
    code, calls, _ = attempt([stalled, stalled, stalled], 2)
    expect(code == stalled and len(calls) == 3,
           f"retries stop after N, with the stalled status, got {code} {len(calls)}")
    code, calls, _ = attempt([1], 2)
    expect(code == 1 and len(calls) == 1, f"a FAIL isn't retried, got {code} {len(calls)}")
    code, calls, waits = attempt([stalled], 0)
    expect(code == stalled and len(calls) == 1 and waits == [],
           f"no retry by default, got {code} {len(calls)} {waits}")


def _run_dir_claim():
    """Two runs that start in the same second get distinct directories, and a name that
    sorts after the bare stamp."""
    out = Path(tempfile.mkdtemp()) / "results"
    first = driver.claim_run_dir(out, now=0)
    second = driver.claim_run_dir(out, now=0)
    third = driver.claim_run_dir(out, now=0)
    expect(len({first, second, third}) == 3 and all(p.is_dir() for p in (first, second, third)),
           f"same-second claims share a directory: {first} {second} {third}")
    expect(first.name == "19700101T000000Z" and second.name == "19700101T000000Z-2",
           f"names are the stamp, then stamp-N: {first.name} {second.name}")
    expect(sorted(p.name for p in (third, second, first)) == [first.name, second.name, third.name],
           "a suffixed name sorts after its bare stamp")
    later = driver.claim_run_dir(out, now=1)
    expect(later.name == "19700101T000001Z", f"a new second starts bare again: {later.name}")


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

    # A `restart`: the old process's final drain at +60.07, the new one started at +60.26. The
    # final drain is the old life's, though it falls within a second of the new life's start.
    restart = "2026-10-08T12:01:%s"
    lines = [
        _line(restart % "00.000000000Z", "logit.process.uptime", "gauge", 55.0),
        _line(restart % "00.070000000Z", "logit.process.uptime", "gauge", 55.07),
        _line(restart % "00.070000000Z", "logit.input.lines", "sum", 4900, component="tail"),
        _line(restart % "05.260000000Z", "logit.process.uptime", "gauge", 5.0),
        _line(restart % "05.260000000Z", "logit.input.lines", "sum", 5000, component="tail"),
    ]
    tel = telemetry.parse_ndjson(lines)
    by_life = tel.counter_by_life("logit.input.lines", component="tail")
    expect(by_life == {0: 4900, 1: 5000},
           f"a restart's old final drain stays in the old life, got {by_life}")


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
        stall_row = [checks.Result("run", checks.FAIL, "", ["stalled: the tick ran late"])]
        expect(driver.exit_code(stall_row, "aborted") == driver.STALLED_EXIT
               and driver.exit_code(stall_row, "interrupted") == 130,
               "a stalled run exits STALLED_EXIT, unless it was interrupted")

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
                topic_format="json",
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
    `(offset, amount)` records to write in place of the four. Under `topic_format`
    `"otlp_proto"` each record is an escaped OTLP metrics request holding one `x_0` data point
    (`_otlp_topic_record`).

    With `standin_edit`, the target is a stand-in: the same export becomes its record, one
    stored `200` request per sample timestamp a second before it (`_standin_record`), the SUT's
    `logit.output.requests` counts each in the drain after it, and `standin_edit(requests)`
    returns the request lines to write, edited. Under `standin_shape` `"datadog"` the record is
    `_datadog_record`'s, the export's samples as delta points, and under `"influxdb"`
    `_influx_record`'s, the same points as line protocol."""
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
                                       "topic": "soak", "format": topic_format})
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
                  else _influx_record(export) if standin_shape == standin.INFLUXDB
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
        write = _otlp_topic_record if topic_format == topic.OTLP_PROTO else _topic_record
        records = [write(T0 + offset, amount) for offset, amount in shares]
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


INFLUX_QUERY = "org=soak-org&bucket=soak&precision=ns"


def _influx_request(seq, offset, points, status=204, stored=True, cue=None, answer="status",
                    query=INFLUX_QUERY, path="/api/v2/write"):
    """One `influxdb` stand-in record line at `T0 + offset` whose body is line protocol of
    `points`, `[(measurement, value, timestamp_ms)]`, each tagged `env=soak` with field
    `value`."""
    body = "".join(f"{name},env=soak value={value!r} {stamp * 1_000_000}\n"
                   for name, value, stamp in points).encode()
    return {"event": "request", "seq": seq, "t": T0 + offset, "path": path, "query": query,
            "content_type": "text/plain; charset=utf-8", "content_encoding": None,
            "bytes": len(body), "cue": cue, "answer": answer,
            "status": None if answer == "reset" else status, "stored": stored,
            "body_b64": base64.b64encode(body).decode()}


def _influx_record(export):
    """The requests an `influxdb` stand-in would record for `export`: one stored `204` per
    sample timestamp, a second before it, holding every series' increment since its previous
    sample in the same SUT life, as `_datadog_record` builds them."""
    by_stamp = {}
    for one in export:
        metric = one["metric"]["__name__"].removesuffix("_total")
        previous = {}
        for stamp, value in zip(one["timestamps"], one["values"]):
            life = 0 if stamp / 1000 < T0 + STOP[1] else 1
            delta = value - previous.get(life, 0)
            previous[life] = value
            by_stamp.setdefault(stamp, []).append((metric, float(delta), stamp))
    return [_influx_request(seq, stamp / 1000 - T0 - 1, points)
            for seq, (stamp, points) in enumerate(sorted(by_stamp.items()), start=1)]


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


# The `split = "target"` fixture's sinks: `(sink, backend, target, aggregate, transforms)`.
# A router, `split`, sends each event whole to both targets; `vm_b`'s branch has one
# transform, `count_b`, ahead of its aggregate.
TARGET_SINKS = (("vm_a", "victoria-metrics", "t_a", "agg_a", []),
                ("vm_b", "victoria-metrics-b", "t_b", "agg_b", ["count_b"]))


def _to_targets(run_dir, adds=(), export_edit=None):
    """Rewrites a `_ledger_run` or `_kill_run` directory for `split = "target"`
    (`TARGET_SINKS`): each event `window` received, the router `split` receives and copies
    once, each target sends, `count_b` forwards, and each sink's `aggregate` receives and
    absorbs; each aggregate sends `AGGREGATE_SENT` events per drain, which its sink receives;
    every line of the fixture's one sink is written once per sink; and the export goes to each
    sink whole. `adds` is `(offset, metric, delta, component, attrs)` points written beside
    the rest, each summing into its drain's count, and `export_edit(sink, series)` may change
    each sink's export."""
    out = []
    received = "logit.component.events.received"
    sent = "logit.component.events.sent"
    for line in (run_dir / "logs" / "logit.stdout").read_text().splitlines():
        record = json.loads(line)
        component = record["attributes"]["component"]
        metric = record["metrics"][0]
        name, value, stamp = metric["name"], metric["value"], record["timestamp"]
        if component == "window" and name == received:
            for metric_name, comp in ((received, "split"), ("logit.component.events.copied",
                                                            "split"),
                                      (sent, "t_a"), (sent, "t_b"), (received, "count_b"),
                                      (sent, "count_b"), (received, "agg_a"),
                                      (received, "agg_b")):
                out.append(_line(stamp, metric_name, "sum", value, component=comp))
        elif component == "window" and name in ("logit.transform.metrics.absorbed",
                                                "logit.component.batches.sent"):
            for comp in ("agg_a", "agg_b"):
                out.append(_line(stamp, name, "sum", value, component=comp))
        elif component == "sink":
            for sink, _, _, _, _ in TARGET_SINKS:
                copied = json.loads(line)
                copied["attributes"]["component"] = sink
                out.append(json.dumps(copied))
        else:
            out.append(line)
            if name == "logit.process.uptime":
                for sink, _, _, aggregate, _ in TARGET_SINKS:
                    out.append(_line(stamp, sent, "sum", AGGREGATE_SENT, component=aggregate))
                    out.append(_line(stamp, received, "sum", AGGREGATE_SENT, component=sink))
    for offset, name, delta, component, attrs in adds:
        out.append(_line(_stamp(T0 + offset), name, "sum", delta, component=component,
                         attrs=attrs))
    (run_dir / "logs" / "logit.stdout").write_text("\n".join(out) + "\n")
    resolved = json.loads((run_dir / "scenario.resolved.json").read_text())
    resolved["services"] = {**scenario.BASE_ROLES, "victoria-metrics-b": "backend"}
    ledger = resolved["ledger"]
    selector = ledger["sink"][0].get("vm_selector")
    for key in ("sut_aggregate", "vm_every_window"):
        ledger.pop(key, None)
    ledger.update(split="target", sut_router="split")
    ledger["sink"] = [{"id": sink, "backend": backend, "vm_selector": selector,
                       "target": target, "aggregate": aggregate, "transforms": list(chain)}
                      for sink, backend, target, aggregate, chain in TARGET_SINKS]
    (run_dir / "scenario.resolved.json").write_text(json.dumps(resolved))
    config = run_dir / "configs" / "logit-sut.yaml"
    if config.is_file():
        config.write_text(config.read_text().replace(
            "  window:\n    type: aggregate\n    interval: 10s   # every window\n",
            "".join(f"  {aggregate}: {{ type: aggregate, interval: 10s }}\n"
                    for _, _, _, aggregate, _ in TARGET_SINKS)))
    series = vm.parse_export((run_dir / "vm-export" / "sink.jsonl").read_text())
    (run_dir / "vm-export" / "sink.jsonl").unlink()
    for sink, _, _, _, _ in TARGET_SINKS:
        mine = copy.deepcopy(series)
        _write_export(run_dir, sink, export_edit(sink, mine) if export_edit else mine)
    return run_dir


def _target_ledger():
    """The ledger under `split = "target"`: the listeners' side ending at the router, the
    router's balance and each of its terms, each sink's branch and egress from its own target,
    a stopped life's exact ingress, per-sink drop credit, the summary's per-term judgment, and
    the per-sink kill band. Each fixture's verdict depends on the rule it names."""
    rows = ("ledger.intake", "ledger.edge", "ledger.aggregate", "ledger.egress",
            "ledger.summary", "drops", "identity.sink", "progress")
    copied = "logit.component.events.copied"
    sent = "logit.component.events.sent"
    received = "logit.component.events.received"
    absorbed = "logit.transform.metrics.absorbed"
    dropped = "logit.component.events.dropped"
    discarded = "logit.component.events.discarded"

    def change(sink_name, life, by):
        """An `export_edit` moving `sink_name`'s V in `life` by `by`, on series x_0."""
        index = {0: (0, 1), 1: (2, 3)}[life]

        def edit(sink, series):
            if sink == sink_name:
                for one in series:
                    if one["metric"]["__name__"] == "x_0_total":
                        for i in index:
                            one["values"][i] += by
            return series
        return edit

    with tempfile.TemporaryDirectory() as tmp:
        def results(name, adds=(), export_edit=None, **kwargs):
            kwargs = {"life0_extra": 20, "shutdown_drops": 20, "v0_extra": 0, **kwargs}
            run_dir = _to_targets(_ledger_run(tmp, name, **kwargs), adds, export_edit)
            return {r.id: r for r in checks.run_all(run_dir)}

        clean = results("targets-clean")
        for row in rows:
            expect(clean[row].status == checks.PASS,
                   f"{row} PASSes the balanced split = target fixture, got {clean[row]}")
        egress = clean["ledger.egress"].detail
        expect("vm_a: life 0: T_sent[t_a] − T_x − V 0 = residual 0 + Ab[agg_a] − V 0, ok"
               in egress and "vm_b: life 0: T_sent[t_b] − T_x − ΣT_disc − V 0" in egress
               and "life 1 (final): Ab[agg_b]" in egress and "split target" in egress,
               f"egress under target judges each sink from its own target, got {egress}")
        edge = clean["ledger.edge"].detail
        expect("R_copied 34,000 − R_disc 0 = 68,000 == R_fwd + ΣT_sent 68,000" in edge
               and "E 34,000 == R_in[split] 34,000" in edge
               and "A_sent[agg_b] 380 == S_in[vm_b] 380" in edge,
               f"edge under target judges the router and each branch, got {edge}")
        expect("life 0 (stopped): W − D − B − X − R_in 0" in clean["ledger.intake"].detail,
               f"intake under target judges a stopped life's ingress with no tolerance, got "
               f"{clean['ledger.intake']}")
        expect("agg_a: Ab 34,000 == A 34,000" in clean["ledger.aggregate"].detail
               and "agg_b: Ab" in clean["ledger.aggregate"].detail,
               f"aggregate under target judges each sink's aggregate, got "
               f"{clean['ledger.aggregate']}")

        # The router's balance, in the final life and in a stopped one, and each of its terms.
        short = results("targets-router-short", adds=[(250, copied, -5, "split", None)])
        expect(short["ledger.edge"].status == checks.FAIL
               and "!= R_fwd + ΣT_sent 68,000, off by -5" in short["ledger.edge"].detail
               and short["ledger.summary"].status == checks.FAIL,
               f"a router copying fewer than its targets sent FAILs edge and summary, got "
               f"{short['ledger.edge']} / {short['ledger.summary']}")
        stopped = results("targets-router-stopped", adds=[(50, copied, 5, "split", None)])
        expect(stopped["ledger.edge"].status == checks.FAIL
               and "life 0: R_in" in stopped["ledger.edge"].detail
               and "off by 5" in stopped["ledger.edge"].detail,
               f"the router's balance is judged in a stopped life, got {stopped['ledger.edge']}")
        def shift(sink, delta, offset=250):
            """`adds` moving `sink`'s branch by `delta` events at `offset`, every hop alike."""
            hops = ([(sent, "t_a"), (received, "agg_a"), (absorbed, "agg_a")] if sink == "vm_a"
                    else [(sent, "t_b"), (received, "count_b"), (sent, "count_b"),
                          (received, "agg_b"), (absorbed, "agg_b")])
            return [(offset, name, delta, comp, None) for name, comp in hops]

        def shifted(deltas, life=1):
            """An `export_edit` moving each sink's V in `life`, the final by default, by its
            delta."""
            def edit(sink, series):
                return change(sink, life, deltas.get(sink, 0))(sink, series)
            return edit

        # 5 events the router kept out of both targets, discarded or sent on its forward edge.
        for name, counter in (("discarded", discarded), ("forward", sent)):
            adds = ([(250, copied, -5, "split", None), (250, counter, 5, "split", None)]
                    + shift("vm_a", -5) + shift("vm_b", -5))
            balanced = results(f"targets-router-{name}", adds=adds,
                               export_edit=shifted({"vm_a": -5, "vm_b": -5}))
            expect(balanced["ledger.edge"].status == checks.PASS
                   and balanced["ledger.summary"].status == checks.PASS,
                   f"the router's balance takes its {name} events in, got "
                   f"{balanced['ledger.edge']} / {balanced['ledger.summary']}")

        # The balance pools the targets: 5 events moved from t_b onto t_a, which already carries
        # every event, leave it and every branch balanced, and only t_a's bound FAILs. A forward
        # edge sending more than the router took in FAILs its own bound the same way.
        moved = results("targets-router-moved", adds=shift("vm_a", 5) + shift("vm_b", -5),
                        export_edit=shifted({"vm_a": 5, "vm_b": -5}))
        expect(moved["ledger.edge"].status == checks.FAIL
               and "T_sent[t_a] 34,005 <= R_in − R_disc 34,000, over its bound"
               in moved["ledger.edge"].detail
               and all(moved[row].status == checks.PASS
                       for row in ("ledger.egress", "ledger.aggregate", "ledger.summary")),
               f"a target sent more than the router took in FAILs its bound alone, got "
               f"{moved['ledger.edge']}")
        # The same move in the stopped life 0 FAILs that life's bound.
        moved0 = results("targets-router-moved-stopped",
                         adds=shift("vm_a", 5, 50) + shift("vm_b", -5, 50),
                         export_edit=shifted({"vm_a": 5, "vm_b": -5}, life=0))
        expect(moved0["ledger.edge"].status == checks.FAIL
               and "life 0: T_sent[t_a] 20,005 <= R_in − R_disc 20,000, over its bound"
               in moved0["ledger.edge"].detail
               and all(moved0[row].status == checks.PASS
                       for row in ("ledger.egress", "ledger.aggregate", "ledger.summary")),
               f"a target sent more than the router took in FAILs its bound in a stopped life, "
               f"got {moved0['ledger.edge']}")
        forward = results("targets-router-forward-over",
                          adds=[(250, copied, 40000, "split", None),
                                (250, sent, 40000, "split", None)])
        expect(forward["ledger.edge"].status == checks.FAIL
               and "R_fwd 40,000 <= R_in 34,000, over its bound" in forward["ledger.edge"].detail,
               f"a forward edge sending more than the router took in FAILs, got "
               f"{forward['ledger.edge']}")

        # A closed consumer's drop at a target, in a stopped life and the final one: the
        # target counted the send, its branch never received it.
        closed = [(offset, name, delta, comp, attrs) for offset in (50, 250)
                  for name, delta, comp, attrs in (
                      (dropped, 5, "t_a", {"reason": "closed_consumer"}),
                      (received, -5, "agg_a", None), (absorbed, -5, "agg_a", None))]
        closed_run = results("targets-closed-consumer", adds=closed,
                             export_edit=lambda sink, series: change(
                                 "vm_a", 0, -5)(sink, change("vm_a", 1, -5)(sink, series)))
        for row in ("ledger.edge", "ledger.egress", "ledger.summary", "drops"):
            expect(closed_run[row].status == checks.PASS,
                   f"{row} takes a target's closed-consumer drop off its sends, got "
                   f"{closed_run[row]}")

        # A branch transform's discard in a stopped life: the sink's egress gap leaves it out.
        branch = results("targets-branch-discard",
                         adds=[(50, discarded, 5, "count_b", None),
                               (50, sent, -5, "count_b", None),
                               (50, received, -5, "agg_b", None),
                               (50, absorbed, -5, "agg_b", None)],
                         export_edit=change("vm_b", 0, -5))
        for row in ("ledger.edge", "ledger.egress", "ledger.summary"):
            expect(branch[row].status == checks.PASS,
                   f"{row} counts a branch transform's discard, got {branch[row]}")

        # A surplus at one sink and a matching loss at the other: each sink's own line FAILs.
        netted = results("targets-netted", export_edit=lambda sink, series: change(
            sink, 1, 20 if sink == "vm_a" else -20)(sink, series))
        egress = netted["ledger.egress"].detail
        expect(netted["ledger.egress"].status == checks.FAIL
               and "V[vm_a] 34,020 = -20, uncounted" in egress
               and "V[vm_b] 33,980 = 20, uncounted" in egress
               and netted["ledger.summary"].status == checks.FAIL,
               f"opposite gaps at two sinks each FAIL, got {netted['ledger.egress']} / "
               f"{netted['ledger.summary']}")

        # One aggregate absorbing less than it received FAILs that aggregate's line.
        short_ab = results("targets-aggregate-short", adds=[(250, absorbed, -3, "agg_b", None)],
                           export_edit=change("vm_b", 1, -3))
        expect(short_ab["ledger.aggregate"].status == checks.FAIL
               and "agg_b: Ab 33,997 != A 34,000" in short_ab["ledger.aggregate"].detail
               and "agg_a: Ab 34,000 == A 34,000" in short_ab["ledger.aggregate"].detail
               and "A − Ab[agg_b] 3" in short_ab["ledger.summary"].detail
               and short_ab["ledger.summary"].status == checks.FAIL,
               f"a short aggregate FAILs by name, got {short_ab['ledger.aggregate']} / "
               f"{short_ab['ledger.summary']}")

        # Two terms that cancel: count_b sends 5 fewer than it received and discarded, and
        # agg_b receives 5 more than count_b sent. Summed they're 0; each FAILs on its own.
        cancel = results("targets-cancel", adds=[(250, sent, -5, "count_b", None)])
        expect(cancel["ledger.summary"].status == checks.FAIL
               and "uncounted 10" in cancel["ledger.summary"].detail
               and cancel["ledger.edge"].status == checks.FAIL,
               f"the summary judges each term alone, got {cancel['ledger.summary']}")

        # A stopped life's ingress must be 0: here 5 lines read and never sent on.
        ingress = results("targets-ingress", life0_extra=25)
        expect(ingress["ledger.intake"].status == checks.FAIL
               and "life 0 (stopped): W − D − B − X − R_in 5, uncounted, not 0"
               in ingress["ledger.intake"].detail,
               f"a stopped life's ingress short of the router FAILs, got "
               f"{ingress['ledger.intake']}")

        # A stopped life's sink gap, credited by a drop on its own branch only.
        for name, dropped_at, verdict in (("own-sink", "vm_a", checks.PASS),
                                          ("own-aggregate", "agg_a", checks.PASS),
                                          ("other-sink", "vm_b", checks.FAIL),
                                          ("other-aggregate", "agg_b", checks.FAIL)):
            gap = results(f"targets-gap-{name}", life0_dropped=1, dropped_at=dropped_at,
                          export_edit=change("vm_a", 0, -7))
            egress = gap["ledger.egress"].detail
            expect(gap["ledger.egress"].status == verdict
                   and ("vm_a: life 0: T_sent[t_a] − T_x − V 7 = residual 0 + Ab[agg_a] − V 7, "
                        + ("counted" if verdict == checks.PASS else "uncounted")) in egress,
                   f"a shutdown drop at {dropped_at} counts vm_a's gap only on its own branch, "
                   f"got {egress}")

    with tempfile.TemporaryDirectory() as tmp:
        def killed(name, adds=(), **kwargs):
            run_dir = _to_targets(_kill_run(tmp, name, every_window=False, replayed=0,
                                            **kwargs), adds)
            return {r.id: r for r in checks.run_all(run_dir)}

        # Each target sending 1,000 more in the killed life's last drain doubles its branch's
        # rate to 400/s and leaves a branch residual of 1,000, so the band is
        # [-3,000, 6,000]; read from the listener's 200/s it would be [-1,000, 3,000].
        rate = killed("targets-kill-rate", kill_gap=4000,
                      adds=[(200, "logit.component.events.sent", 1000, target, None)
                            for target in ("t_a", "t_b")])
        expect(rate["ledger.egress"].status == checks.PASS
               and "= 4,000, within band (band [-3,000, 6,000]: ingest 400/s"
               in rate["ledger.egress"].detail,
               f"a killed life's band reads each branch's own rate and residual, got "
               f"{rate['ledger.egress']}")

        clean = killed("targets-kill")
        egress = clean["ledger.egress"].detail
        expect(clean["ledger.egress"].status == checks.PASS
               and "life 1 (killed): Ab[agg_a] 14,000 − V[vm_a] 12,500 = 1,500, within "
               "band (band [-1,000, 3,000]" in egress
               and "residual T_sent[t_b] − T_x − ΣT_disc − Ab 0" in egress,
               f"a killed life's sink gap is judged in its branch's band, got {egress}")
        lost = killed("targets-kill-lost", kill_gap=5000)
        expect(lost["ledger.egress"].status == checks.FAIL
               and "= 5,000, uncounted" in lost["ledger.egress"].detail,
               f"a killed life's sink gap above its band FAILs, got {lost['ledger.egress']}")


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
        # 16,000 over the 90 s window is 640,000 an hour.
        result = one("rate is the window's delta per hour", checks.PASS, reduce="rate",
                     min=639999, max=640001)
        expect("16,000 over 90s" in result.detail, f"rate names its delta and span, got {result}")
        one("rate over its max FAILs", checks.FAIL, reduce="rate", max=639000)
        one("rate of a loss counter never emitted reads 0", checks.PASS, reduce="rate",
            metric="logit.input.kernel.dropped", max=0)
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


def _rss_settle_rules(root):
    """`rss_settle` defaults to 0 s, loads a duration, reaches the resolved scenario, and is
    refused negative or unparsable."""
    path, base = _base(root)

    def with_settle(value):
        raw = copy.deepcopy(base)
        raw["rss_settle"] = value
        return raw

    loaded = scenario.from_dict(copy.deepcopy(base), path)
    expect(loaded.rss_settle == scenario.DEFAULT_RSS_SETTLE_S == 0.0
           and loaded.to_json()["rss_settle"] == 0.0,
           f"rss_settle defaults to 0s in the resolved scenario, got {loaded.rss_settle}")
    for value, seconds in (("90s", 90.0), ("0s", 0.0), ("2m30s", 150.0)):
        loaded = scenario.from_dict(with_settle(value), path)
        expect(loaded.rss_settle == seconds and loaded.to_json()["rss_settle"] == seconds,
               f"rss_settle = {value!r} loads as {seconds:g}s, got {loaded.rss_settle}")
    for value in ("-30s", "soon", "30", 30):
        _refused(path, with_settle(value), "scenario.rss_settle: not a duration",
                 f"rss_settle = {value!r}")
    # Only these schedules opt in; every other shipped scenario keeps recovery_bound alone.
    opted_in = {"fanin": 60.0, "tcp-ingress": 60.0, "partition-rss": 120.0}
    for path_one in sorted((root / "scenarios").glob("*/scenario.toml")):
        name = path_one.parent.name
        want = opted_in.get(name, 0.0)
        shipped = scenario.load(path_one)
        expect(shipped.rss_settle == want and shipped.to_json()["rss_settle"] == want,
               f"{name} loads rss_settle = {want:g}s, got {shipped.rss_settle}")
    loaded = scenario.from_dict(copy.deepcopy(base), path)
    loaded.rss_settle = -1.0
    try:
        scenario.validate(loaded)
        expect(False, "validate() accepts a negative rss_settle")
    except scenario.ScenarioError as err:
        expect(any("rss_settle must be 0s or more" in p for p in err.problems),
               f"validate() refuses a negative rss_settle, got {err.problems}")


def _settle_run(tmp, name, steps, end_s, sut, gen, warmup=60.0, **resolved):
    """A run directory of `end_s` seconds with `steps`, `[(step, action, on, start, end)]`,
    and `logit` and `generator` NDJSON whose resident bytes are `sut(offset)` and
    `gen(offset)`."""
    timeline = [_phase("start", 0, t0=T0)]
    for step, action, on, start, end in steps:
        timeline += [_action("apply", step, action, on, start, start + 1),
                     _action("revert", step, action, on, end, end)]
    timeline += [_phase("end_begin", end_s), _phase("end_end", end_s + 50)]
    flat = {"fds": lambda o: 10, "strings": lambda o: 100}
    run_dir = _run_dir(tmp, name, timeline, _service_stdout(end_s + 50, rss=sut, **flat),
                       duration=float(end_s), warmup=warmup, **resolved)
    (run_dir / "logs" / "generator.stdout").write_text(
        "\n".join(_service_stdout(end_s + 50, rss=gen, **flat)) + "\n")
    return run_dir


def _rss_settle_checks():
    """`rss_slope` excuses `rss_settle` past `recovery_bound` after each fault: a backlog's RSS
    returned in steps inside it PASSes, the same run with `rss_settle = 0s` doesn't, growth
    across the whole run still FAILs, and a schedule the window covers whole WARNs on
    coverage rather than SKIPping."""
    mib = 2 ** 20

    def flat(o):
        return 50 * mib
    with tempfile.TemporaryDirectory() as tmp:
        # fanin's generator around its partition (20261018T190542Z): 18 MiB, 86 MiB at the
        # revert, a 62.6 MiB plateau from +14 s, 52.6 MiB from +84 s, 32 MiB from +94 s, and
        # back at 18 MiB from +100 s, inside recovery_bound 45 s + rss_settle 60 s.
        def stepped(o):
            after = o - 375
            if 330 <= o < 389:
                return 86 * mib
            if 0 <= after < 84:
                return 62.6 * mib
            if 84 <= after < 94:
                return 52.6 * mib
            if 94 <= after < 100:
                return 32 * mib
            return 18 * mib
        partition = [("c0s1", "partition", "generator", 330, 375)]
        settled = _settle_run(tmp, "settle-bump", partition, 600, flat, stepped,
                              rss_settle=60.0)
        result = _status(settled, checks.check_rss_slope)
        expect(result.status == checks.PASS and "generator PASS" in result.detail,
               f"a fault's RSS returned within rss_settle PASSes, got {result}")
        unsettled = _settle_run(tmp, "settle-bump-off", partition, 600, flat, stepped,
                                rss_settle=0.0)
        result = _status(unsettled, checks.check_rss_slope)
        expect(result.status == checks.WARN and "generator WARN" in result.detail,
               f"the same RSS with rss_settle 0s WARNs on its slope, got {result}")
        # A run directory recorded before the key scores as it did then, with no settle.
        unkeyed = _settle_run(tmp, "settle-bump-unkeyed", partition, 600, flat, stepped)
        data = checks.RunData(unkeyed)
        result = checks.check_rss_slope(data)
        expect(data.rss_settle == 0.0 and result.status == checks.WARN,
               f"a run directory with no rss_settle reads 0s, got {data.rss_settle}, {result}")

        # cardinality's leak (20261012T215005Z): the SUT +73 and the generator +61 MiB/h for
        # 75 minutes against a 48 MiB/h limit, its five faults each adding 40 MiB that comes
        # back inside the settle window.
        faults_at = [("c0s1", "netem", "logit", 300, 420),
                     ("c0s2", "stop", "victoria-metrics", 900, 990),
                     ("c1s1", "netem", "logit", 2100, 2220),
                     ("c1s2", "stop", "victoria-metrics", 2700, 2790),
                     ("c2s1", "netem", "logit", 3900, 4020)]

        def leaking(base, per_hour):
            def rss(o):
                bump = any(start <= o < end + 90 for _, _, _, start, end in faults_at)
                return base * mib + per_hour * mib * o / 3600 + (40 * mib if bump else 0)
            return rss
        thresholds = {**RESOLVED["thresholds"], "rss_growth_mib_per_hour": 48}
        leak = _settle_run(tmp, "settle-leak", faults_at, 4500, leaking(50, 73),
                           leaking(30, 61), warmup=120.0, cooldown=180.0,
                           thresholds=thresholds, rss_settle=60.0)
        data = checks.RunData(leak)
        result = checks.check_rss_slope(data)
        expect(data.rss_settle == 60.0 and result.status == checks.FAIL
               and "logit FAIL: MiB/h life 0 +73.0" in result.detail
               and "generator FAIL: MiB/h life 0 +61.0" in result.detail,
               f"growth across the whole run FAILs with rss_settle in force, got {result}")

        # Faults every 150 s for 30 s: rss_settle leaves 15 s after each, two samples, so a
        # life fits over 30 s judged of the 900 s after warmup.
        dense = [(f"c{i}s1", "netem", "generator", start, start + 30)
                 for i, start in enumerate(range(60, 811, 150))]
        covered = _settle_run(tmp, "settle-dense", dense, 960, flat, flat, rss_settle=60.0)
        result = _status(covered, checks.check_rss_slope)
        expect(result.status == checks.WARN and "cover little" in " ".join(result.lines)
               and "every sample after warmup in a fault window" not in result.detail,
               f"a schedule rss_settle leaves too little of WARNs on coverage, got {result}")
        whole = [("c0s1", "netem", "generator", 60, 1000)]
        excused = _settle_run(tmp, "settle-whole", whole, 1000, flat, flat, rss_settle=60.0)
        result = _status(excused, checks.check_rss_slope)
        expect(result.status == checks.WARN
               and "every sample after warmup in a fault window" in result.detail,
               f"a service with every sample excused WARNs rather than SKIPs, got {result}")


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
    _refused(path, with_expect(metric="logit.process.memory.resident.bytes", reduce="rate"),
             "logit.process.memory.resident.bytes is a gauge", "a rate of a gauge")
    loaded = scenario.from_dict(with_expect(metric="logit.output.reconnects", reduce="rate"),
                                path)
    expect(loaded.expectations[0].reduce == "rate", "a rate of a counter loads")


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
        expect(code == 2 and not private.exists() and not private.parent.exists(),
               "execute() exits 2 on a busy project, and removes the private env copy")
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
           and loaded.target == {"kind": "kafka", "name": "kafka", "env": [], "topic": "soak",
                                 "format": "json"},
           f"kafka-broker-restart loads as a Kafka target, got {loaded.target}")
    roles = scenario.base_roles(loaded)
    expect(roles == {"logit": "sut", "generator": "generator", "kafka": "broker"},
           f"a Kafka target's stack runs the broker kafka and no backend, got {roles}")
    expect(loaded.sinks == [{"id": "kafka"}],
           f"kafka-broker-restart's one sink names no backend, got {loaded.sinks}")
    expect(loaded.to_json()["target"]["topic"] == "soak",
           "the resolved scenario records the topic")
    otlp_path = root / "scenarios" / "kafka-otlp-broker-restart" / "scenario.toml"
    otlp_loaded = scenario.load(otlp_path)
    expect(otlp_loaded.to_json()["target"]["format"] == topic.OTLP_PROTO
           and loaded.to_json()["target"]["format"] == topic.JSON,
           "the resolved target records the format its sink's config writes")
    sinks = {"sink": [{"id": "kafka"}]}

    def fmt(body, topic_name="soak"):
        problems = []
        found = scenario.kafka_format(sinks, f"components:\n  kafka:\n{body}", topic_name,
                                      problems)
        return found, problems
    head = "    type: kafka_out\n    brokers: [\"kafka:9092\"]\n"
    format_cases = [
        (head + "    topic: soak\n", topic.JSON, None, "no format: is json"),
        (head + "    format: otlp_proto  # format: json\n    topics: { metrics: soak }\n",
         topic.OTLP_PROTO, None, "otlp_proto writing its metrics topic"),
        (head + "    format: otlp_proto\n    topic: soak\n", topic.OTLP_PROTO, None,
         "otlp_proto with every signal on one topic"),
        (head + "    format: otlp_proto\n", topic.OTLP_PROTO,
         "writes its metrics to 'otlp_metrics'", "otlp_proto's default metrics topic"),
        (head + "    topic: other\n", topic.JSON, "but ledger.sink 'kafka' writes 'other'",
         "a json sink on another topic"),
        (head + "    topic: soak\n    format: msgpack\n", topic.JSON,
         "writes format msgpack; the ledger reads a topic in json or otlp_proto only",
         "a format the ledger can't read"),
        (head + "    format: otlp_json\n    topic: soak\n", topic.JSON,
         "writes format otlp_json", "otlp_json, which the ledger doesn't read"),
        ("    type: stdio_out\n", topic.JSON, "is a stdio_out", "a sink that isn't kafka_out"),
    ]
    for body, want, needle, what in format_cases:
        found, problems = fmt(body)
        expect(found == want and (any(needle in p for p in problems) if needle
                                  else not problems),
               f"{what}: format {found}, problems {problems}")
    missing = []
    scenario.kafka_format({"sink": [{"id": "gone"}]}, "components:\n" + head, "soak", missing)
    expect(any("'gone' isn't a component" in p for p in missing),
           f"a sink the SUT config doesn't hold is refused, got {missing}")
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
    for name, action in faults.ACTIONS.items():
        if name in scenario.NO_WINDOW_ACTIONS:
            continue
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
    records, unparsed, copies, _ = topic.parse_records("\n".join(lines) + "\n\n")
    expect(len(records) == 4 and unparsed == 2 and copies == [],
           f"two of six lines aren't JSON objects, got {records} {unparsed} {copies}")
    prefixed, _, _, _ = topic.parse_records("\n".join(lines), prefix="x_")
    expect([amount for _, amount in prefixed] == [30, 0, 70, 0],
           f"only sums under the prefix count, got {prefixed}")
    by_life, total = topic.totals_by_life(records, [900, 2000])
    expect(by_life == {0: 35, 1: 70} and total == 105,
           f"each record goes to the life its timestamp falls in; a gauge adds 0, got "
           f"{by_life} {total}")
    resent = lines[:3] + [lines[2], lines[0], lines[2]]
    records, _, copies, _ = topic.parse_records("\n".join(resent))
    by_life, total = topic.totals_by_life(records, [900, 2000])
    expect(len(records) == 3 and total == 105
           and topic.duplicates_by_life(copies, [900, 2000]) == {0: 1, 1: 2},
           f"a record's copies count once in V and per life as duplicates, got {records} "
           f"{copies}")


def _pb(number, wire, payload):
    """One protobuf field: `payload` an int for a varint, else the raw bytes of a 64-bit,
    32-bit, or length-delimited field, whose length this writes."""
    def varint(value):
        out = bytearray()
        value &= (1 << 64) - 1
        while value > 0x7F:
            out.append(value & 0x7F | 0x80)
            value >>= 7
        out.append(value)
        return bytes(out)
    key = varint(number << 3 | wire)
    if wire == otlp.VARINT:
        return key + varint(payload)
    if wire == otlp.LEN:
        return key + varint(len(payload)) + payload
    return key + payload


def _pb_kv(key, value):
    """An OTLP `KeyValue` holding a string."""
    return _pb(1, otlp.LEN, key.encode()) + _pb(2, otlp.LEN, _pb(1, otlp.LEN, value.encode()))


def _otlp_point(time_ns, value, attrs=(), extra=b""):
    """A `NumberDataPoint`: `as_double` for a float `value`, `as_int` for an int, neither for
    None."""
    body = b"".join(_pb(7, otlp.LEN, _pb_kv(k, v)) for k, v in attrs)
    body += _pb(3, otlp.I64, struct.pack("<Q", time_ns))
    if isinstance(value, float):
        body += _pb(4, otlp.I64, struct.pack("<d", value))
    elif value is not None:
        body += _pb(6, otlp.I64, struct.pack("<q", value))
    return body + extra


def _otlp_metric(name, points, kind="sum", monotonic=True, extra=b""):
    """A `Metric` holding a delta `Sum` or a `Gauge` of `points`."""
    data = b"".join(_pb(1, otlp.LEN, point) for point in points)
    if kind == "sum":
        data += _pb(2, otlp.VARINT, otlp.DELTA) + (_pb(3, otlp.VARINT, 1) if monotonic else b"")
        return _pb(1, otlp.LEN, name.encode()) + _pb(7, otlp.LEN, data) + extra
    return _pb(1, otlp.LEN, name.encode()) + _pb(5, otlp.LEN, data) + extra


def _otlp_request(metrics, resource=(("host.name", "soak"),), extra=b""):
    """An `ExportMetricsServiceRequest` of `metrics` under one resource and an empty scope."""
    res = b"".join(_pb(1, otlp.LEN, _pb_kv(k, v)) for k, v in resource)
    scope = _pb(1, otlp.LEN, b"") + b"".join(_pb(2, otlp.LEN, m) for m in metrics)
    return _pb(1, otlp.LEN, _pb(1, otlp.LEN, res) + _pb(2, otlp.LEN, scope)) + extra


def _otlp_topic_record(t, amount, name="x_0"):
    """One `kafka_out` `format: otlp_proto` record as the consumer prints it: an `aggregate`
    delta window's one series as an escaped metrics request."""
    point = _otlp_point(int(round(t * 1e9)), amount)
    return otlp.escape(_otlp_request([_otlp_metric(name, [point])]))


# The `otlp_proto` record `kafka_out` writes for the batch in
# `crates/logit-outputs/src/kafka/encode.rs`'s
# `the_soak_otlp_metrics_vector_is_what_the_encoder_writes`, which pins this file to the encoder.
OTLP_VECTOR = "kafka-otlp-metrics.hex"
OTLP_VECTOR_TS = 1_791_720_000_250_000_000


def _otlp_vector(root):
    text = (root / "vectors" / OTLP_VECTOR).read_text()
    return bytes.fromhex("".join(line for line in text.splitlines()
                                 if not line.startswith("#")))


def _otlp_walker(root):
    """The protobuf walker against the encoder's own bytes: a delta `Sum` of `as_int` and of
    `as_double`, a negative non-monotonic one, a `Gauge`, and a histogram whose packed fields
    are skipped; then fields it doesn't know at every level, unpacked repeated fields, a oneof's
    last member, a merged message, the consumer's escaped form, and bytes it must reject."""
    vector = _otlp_vector(root)
    points = otlp.metric_points(vector)
    got = [(p.kind, p.name, p.value, type(p.value).__name__, p.time_unix_nano, p.temporality,
            p.monotonic) for p in points]
    want = [("sum", "soak_requests_1", 30, "int", OTLP_VECTOR_TS, otlp.DELTA, True),
            ("sum", "soak_requests_2", 2.5, "float", OTLP_VECTOR_TS, otlp.DELTA, True),
            ("gauge", "soak_gauge", 9.0, "float", OTLP_VECTOR_TS, 0, False),
            ("sum", "soak_requests_3", -4, "int", OTLP_VECTOR_TS, otlp.DELTA, False)]
    expect(got == want, f"the encoder's request walks to its four number points, the "
                        f"histogram skipped, got {got}")
    series = [p.series for p in points]
    expect(len(set(series)) == 4 and series[1][2] and not series[0][2]
           and all(s[0] == series[0][0] and s[0] for s in series),
           f"each point's series is its resource, name, and attributes, got {series}")
    line = otlp.escape(vector)
    expect("\n" not in line and otlp.unescape(line) == vector
           and all(" " <= ch <= "~" for ch in line),
           "the consumer's escaped form is one printable line that unescapes to the bytes")
    expect(otlp.unescape("a\\x5Cb\\x0A") == b"a\\b\n" and otlp.escape(b"\\") == "\\x5C",
           "a backslash is always escaped, so every escape is unambiguous")
    for bad in ("\\x5c", "\\x0", "\\y00", "café"):
        try:
            otlp.unescape(bad)
            expect(False, f"{bad!r} isn't the escaped form")
        except otlp.ProtoError:
            pass

    t = 1_000_000_000
    plain = _otlp_request([
        _otlp_metric("a", [_otlp_point(t, 5, [("k", "v")])]),
        _otlp_metric("b", [_otlp_point(t, 1.5)], kind="gauge")])
    unknown = _otlp_request([
        _otlp_metric("a", [_otlp_point(t, 5, [("k", "v")],
                                       extra=_pb(20, otlp.I64, b"\0" * 8)
                                       + _pb(21, otlp.I32, b"\1\2\3\4"))],
                     extra=_pb(15, otlp.LEN, b"xyz") + _pb(16, otlp.VARINT, 300)),
        _otlp_metric("b", [_otlp_point(t, 1.5)], kind="gauge")],
        extra=_pb(99, otlp.VARINT, 7))

    def shape(buf):
        return [(p.kind, p.name, p.value, p.time_unix_nano, p.series[1:]) for p in
                otlp.metric_points(buf)]
    expect(shape(unknown) == shape(plain) and len(shape(plain)) == 2,
           f"fields the walker doesn't read are skipped at every level, got {shape(unknown)}")
    histogram = lambda counts: _pb(1, otlp.LEN, b"h") + _pb(9, otlp.LEN, _pb(1, otlp.LEN, (
        _pb(3, otlp.I64, struct.pack("<Q", t)) + counts)))
    packed = _pb(6, otlp.LEN, struct.pack("<QQ", 2, 3))
    unpacked = _pb(6, otlp.I64, struct.pack("<Q", 2)) + _pb(6, otlp.I64, struct.pack("<Q", 3))
    beside = _otlp_metric("a", [_otlp_point(t, 5)])
    expect(shape(_otlp_request([histogram(packed), beside]))
           == shape(_otlp_request([histogram(unpacked), beside]))
           == shape(_otlp_request([beside])),
           "a histogram's packed and unpacked bucket counts are both skipped")
    last = _pb(1, otlp.LEN, b"m") + _pb(5, otlp.LEN, _pb(1, otlp.LEN, _otlp_point(t, 1))) + _pb(
        7, otlp.LEN, _pb(1, otlp.LEN, _otlp_point(t, 2)) + _pb(2, otlp.VARINT, otlp.DELTA))
    expect([(p.kind, p.value) for p in otlp.metric_points(_otlp_request([last]))] == [("sum", 2)],
           "a oneof keeps its last member")
    both = _otlp_point(t, 7) + _pb(4, otlp.I64, struct.pack("<d", 0.25))
    merged = _pb(1, otlp.LEN, b"m") + _pb(7, otlp.LEN, _pb(1, otlp.LEN, both)) + _pb(
        7, otlp.LEN, _pb(1, otlp.LEN, _otlp_point(t, 3)) + _pb(3, otlp.VARINT, 1))
    got = [(p.value, p.monotonic) for p in otlp.metric_points(_otlp_request([merged]))]
    expect(got == [(0.25, True), (3, True)],
           f"a point's value keeps its last field, and a repeated `sum` merges, got {got}")
    expect([p.value for p in otlp.metric_points(_otlp_request([_otlp_metric(
        "m", [_otlp_point(t, None)])]))] == [0], "a point with no value reads as 0")

    rejects = [
        (vector[:-1], "the encoder's request one byte short"),
        (vector[:len(vector) // 2], "the encoder's request cut in half"),
        (_pb(1, otlp.LEN, b"\x0a\x05ab"), "a resource cut inside its length"),
        (b"\x08" + b"\xff" * 10 + b"\x01", "an eleven-byte varint"),
        (b"\x0b", "a group wire type"),
        (b"\x00\x00", "field number 0"),
        (_otlp_request([_pb(1, otlp.LEN, b"m") + _pb(7, otlp.LEN, _pb(1, otlp.LEN, _pb(
            3, otlp.VARINT, 5)))]), "a time_unix_nano sent as a varint"),
        (_otlp_request([_pb(1, otlp.LEN, b"\xff")]), "a metric name that isn't UTF-8"),
    ]
    for buf, what in rejects:
        try:
            otlp.metric_points(buf)
            expect(False, f"the walker rejects {what}")
        except otlp.ProtoError:
            pass


def _otlp_topic():
    """The topic read under `otlp_proto`: the consumer's deserializer, records deduplicated on
    raw bytes, points on series and time, the prefix, life credit by latest point time, and
    what doesn't decode."""
    argv = topic.consumer_argv("soak", 9, topic.OTLP_PROTO)
    expect("--value-deserializer" in argv
           and argv[argv.index("--value-deserializer") + 1] == topic.BYTES_DESERIALIZER
           and argv[-2:] == ["--max-messages", "9"]
           and "--value-deserializer" not in topic.consumer_argv("soak", 9),
           "an otlp_proto topic is read through BytesDeserializer, a json one as text")
    t = lambda seconds: int(seconds * 1e9)
    window = lambda at, a, b: _otlp_request([
        _otlp_metric("x_0", [_otlp_point(t(at), a)]),
        _otlp_metric("x_1", [_otlp_point(t(at), b, [("shard", "1")])]),
        _otlp_metric("other", [_otlp_point(t(at), 1000)]),
        _otlp_metric("x_g", [_otlp_point(t(at), 50)], kind="gauge")])
    first, second = window(1000, 3, 4), window(2500, 10, 0.5)
    # A different split of `second`: its x_1 point alone.
    resplit = _otlp_request([_otlp_metric("x_1", [_otlp_point(t(2500), 0.5, [("shard", "1")])])])
    lines = [otlp.escape(first), otlp.escape(second), otlp.escape(first), otlp.escape(resplit),
             "not\\escaped", otlp.escape(b"\x0a\x03"), ""]
    read = topic.parse_records("\n".join(lines), prefix="x_", fmt=topic.OTLP_PROTO)
    expect(read.unparsed == 2 and len(read.records) == 3 and read.duplicates == [1000.0]
           and read.point_copies == [2500.0],
           f"one byte copy, one repeated point, two lines that don't decode, got {read}")
    expect([amount for _, amount in read.records] == [7, 10.5, 0],
           f"only Sum points under the prefix count, each series and time once, got "
           f"{read.records}")
    by_life, total = topic.totals_by_life(read.records, [900, 2000])
    expect(by_life == {0: 7, 1: 10.5} and total == 17.5,
           f"each record goes to the life its points' time falls in, got {by_life} {total}")
    empty = topic.parse_records(otlp.escape(_otlp_request([])), fmt=topic.OTLP_PROTO)
    expect(empty.records == [(None, 0)] and not empty.unparsed,
           f"a request with no points is a record with no time, got {empty}")


def _otlp_checks():
    """The checks over a Kafka two-life run whose topic is `otlp_proto`: V from the distinct
    data points, a byte copy and a repeated point each FAILing `ledger.topic` with no epoch
    bump while V counts them once, and a record that doesn't decode FAILing `ledger.egress`."""
    with tempfile.TemporaryDirectory() as tmp:
        def results(name, **kwargs):
            kwargs.setdefault("sut_extra", _kafka_lines())
            run_dir = _ledger_run(tmp, name, kafka=True, topic_format=topic.OTLP_PROTO,
                                  **kwargs)
            return {r.id: r for r in checks.run_all(run_dir)}

        clean = results("otlp-clean")
        for check_id in ("ledger.egress", "ledger.summary", "ledger.topic"):
            expect(clean[check_id].status == checks.PASS,
                   f"{check_id} PASSes a balanced otlp_proto run, got {clean[check_id]}")
        expect("(otlp_proto)" in clean["ledger.topic"].lines[0]
               and "data points' delta sums" in "\n".join(clean["ledger.egress"].lines),
               f"the rows name the format, got {clean['ledger.topic'].lines} "
               f"{clean['ledger.egress'].lines}")
        led = checks.Ledger(checks.RunData(Path(tmp) / "otlp-clean"))
        expect(led.V == {"sink": {0: led.get("Ab", 0) + 30, 1: led.get("Ab", 1)}},
               f"V is the topic's data points per life, got {led.V}")

        def shares(v0, v1):
            return [(50, v0 // 2), (100, v0 - v0 // 2), (200, v1 // 2), (300, v1 - v1 // 2)]
        copied = results("otlp-copy", topic_records=lambda v0, v1: shares(v0, v1)
                         + [shares(v0, v1)[3]])
        expect(copied["ledger.egress"].status == checks.PASS
               and copied["ledger.topic"].status == checks.FAIL
               and "life 1: 1 duplicate copies with no epoch bump"
               in copied["ledger.topic"].detail,
               f"a byte copy counts once in V and FAILs ledger.topic, got "
               f"{copied['ledger.egress']} {copied['ledger.topic']}")
        # The same x_0 point as the record at +300, in a record whose bytes differ.
        run_dir = _ledger_run(tmp, "otlp-resplit", kafka=True, topic_format=topic.OTLP_PROTO,
                              sut_extra=_kafka_lines(accepted=5))
        path = run_dir / topic.TOPIC_FILE
        lines = path.read_text().splitlines()
        stored = otlp.unescape(lines[3])
        lines.append(otlp.escape(stored + _pb(99, otlp.VARINT, 1)))
        path.write_text("\n".join(lines) + "\n")
        resplit = {r.id: r for r in checks.run_all(run_dir)}
        expect(resplit["ledger.egress"].status == checks.PASS
               and resplit["ledger.topic"].status == checks.FAIL
               and "life 1: 1 data point(s) a distinct record repeats with no epoch bump"
               in resplit["ledger.topic"].detail,
               f"a point a distinct record repeats counts once in V and FAILs ledger.topic, "
               f"got {resplit['ledger.egress']} {resplit['ledger.topic']}")
        garbled = results("otlp-garbled", topic_extra=["\\x0A\\x05ab"])
        expect(garbled["ledger.egress"].status == checks.FAIL
               and "1 topic record(s) aren't OTLP metrics requests"
               in garbled["ledger.egress"].detail,
               f"a record that doesn't decode FAILs, got {garbled['ledger.egress']}")


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


def _discard(run_dir, chain, node, offset, n, sent=None):
    """Makes `node` of `chain` discard `n` events in the SUT drain at `offset`, in place: its
    `events.discarded`, and `sent` events (default `n`) fewer in its `events.sent`, in each
    later node's received and sent, in `window`'s received and absorbed, and in `x_0_total`'s
    samples from `offset` to the end of that SUT life (`STOP` splits the two). A `sent` other
    than `n` leaves the node off its balance."""
    sent = n if sent is None else sent
    later = list(chain[chain.index(node) + 1:])
    stamp = _stamp(T0 + offset)
    stdout = run_dir / "logs" / "logit.stdout"
    out = []
    for line in stdout.read_text().splitlines():
        record = json.loads(line)
        component, metric = record["attributes"]["component"], record["metrics"][0]
        if record["timestamp"] == stamp and (
                (component == node and metric["name"] == "logit.component.events.sent")
                or (component in later and metric["name"] in (
                    "logit.component.events.received", "logit.component.events.sent"))
                or (component == "window" and metric["name"] in (
                    "logit.component.events.received", "logit.transform.metrics.absorbed"))):
            metric["value"] -= sent
        out.append(json.dumps(record))
    out.append(_line(stamp, "logit.component.events.discarded", "sum", n, component=node))
    stdout.write_text("\n".join(out) + "\n")
    path = run_dir / "vm-export" / "sink.jsonl"
    series = vm.parse_export(path.read_text())
    same_life = (lambda s: s < STOP[1]) if offset < STOP[1] else (lambda s: s >= STOP[1])
    for one in series:
        if one["metric"]["__name__"] != "x_0_total":
            continue
        one["values"] = [value - sent if offset <= ms / 1000 - T0 and same_life(ms / 1000 - T0)
                         else value for value, ms in zip(one["values"], one["timestamps"])]
    _write_export(run_dir, "sink", series)
    return run_dir


def _with_chain(run_dir, nodes):
    """Puts `nodes` between `_ledger_run`'s listener `in` and `window`, in place: each node
    receives and sends what `in` sent, and the source names them as its `sut_transforms`."""
    stdout = run_dir / "logs" / "logit.stdout"
    out = []
    for line in stdout.read_text().splitlines():
        out.append(line)
        record = json.loads(line)
        if (record["attributes"]["component"] == "in"
                and record["metrics"][0]["name"] == "logit.component.events.sent"):
            for node in nodes:
                for name in ("logit.component.events.received", "logit.component.events.sent"):
                    hop = copy.deepcopy(record)
                    hop["attributes"]["component"] = node
                    hop["metrics"][0]["name"] = name
                    out.append(json.dumps(hop))
    stdout.write_text("\n".join(out) + "\n")
    resolved_path = run_dir / "scenario.resolved.json"
    resolved = json.loads(resolved_path.read_text())
    resolved["ledger"]["source"][0]["sut_transforms"] = list(nodes)
    resolved_path.write_text(json.dumps(resolved))
    return run_dir


def _sources_run(tmp, name, sut_edit=None, gen_edit=None, transforms=(), sut_config=None,
                 timeline_extra=(), discard=(), **kwargs):
    """`_ledger_run`'s two-life run directory with a second source (`LINES_SOURCE`): the
    generator `generator-lines` repeats `generator`'s telemetry, the SUT listener `lines`
    repeats `in`'s lines (its stderr shutdown lines included), and `window`'s received and
    absorbed counts and the export's values double, so every identity holds over the two.

    `sut_edit(record)` may change or drop (return None) each of `lines`' telemetry records, and
    `gen_edit(record)` each of `generator-lines`'. `transforms` names nodes between `lines` and
    `window`, each forwarding what `lines` sent, written as that source's `sut_transforms`;
    `discard` lists `_discard`'s `(node, offset, n[, sent])` for nodes among them;
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
    for one in discard:
        _discard(run_dir, transforms, *one)
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

        # Transforms between `lines` and `window`: each sends what it doesn't discard, else the
        # edge FAILs at the hop.
        chain = ("parse", "count")
        chained = results("sources-transforms", transforms=chain)
        expect(chained["ledger.edge"].status == checks.PASS
               and "T_in[count] 34,000 − T_disc[count] 0 = 34,000 == T_out[count] 34,000 "
               "(count sends what it doesn't discard)" in chained["ledger.edge"].detail,
               f"ledger.edge walks a source's transforms, got {chained['ledger.edge']}")
        expect("10,000 + 132 x 1,000 (lines)" in chained["ledger.intake"].detail,
               f"each transform adds an inbox and a batch to R, got "
               f"{chained['ledger.intake'].detail}")
        dropped = results("sources-transforms-drop", transforms=chain,
                          sut_edit=lambda record: (_at(200, less(
                              "logit.component.events.sent", 3, component="count"))(record)))
        detail = dropped["ledger.edge"].detail
        expect(dropped["ledger.edge"].status == checks.FAIL
               and "T_in[count] 34,000 − T_disc[count] 0 = 34,000 != T_out[count] 33,997"
               in detail,
               f"a transform that loses events uncounted FAILs the edge at its hop, got {detail}")

        # `parse` discards 7 events in the final life, counted, and sends 7 fewer: every row
        # balances, with the discards shown.
        final = results("sources-discard", transforms=chain, discard=[("parse", 200, 7)])
        for row in ("ledger.intake", "ledger.edge", "ledger.aggregate", "ledger.egress",
                    "ledger.summary"):
            expect(final[row].status == checks.PASS,
                   f"{row} PASSes a counted discard in the final life, got {final[row]}")
        expect("T_in[parse] 34,000 − T_disc[parse] 7 = 33,993 == T_out[parse] 33,993"
               in final["ledger.edge"].detail
               and "Σ(sent into aggregate) 67,993 == A 67,993" in final["ledger.edge"].detail,
               f"ledger.edge shows the node's discards, got {final['ledger.edge'].detail}")
        summary = final["ledger.summary"].detail
        expect("E − ΣT_disc − A 0" in summary
               and "the transforms' ΣT_disc 7, counted" in summary,
               f"ledger.summary takes the discards out of the path term, got {summary}")
        # A discard counted beside an event sent anyway, and one beside two more events lost.
        for name, sent, off in (("sources-discard-sent", 0, "-3"),
                                ("sources-discard-short", 5, "2")):
            unbalanced = results(name, transforms=chain, discard=[("count", 200, 3, sent)])
            detail = unbalanced["ledger.edge"].detail
            expect(unbalanced["ledger.edge"].status == checks.FAIL
                   and f"T_in[count] 34,000 − T_disc[count] 3 = 33,997 != T_out[count] "
                   f"{34_000 - sent:,}, off by {off}" in detail,
                   f"a discard count off the node's sends FAILs the edge, got {detail}")
            expect(unbalanced["ledger.summary"].status == checks.FAIL
                   and f"E − ΣT_disc − A {off}" in unbalanced["ledger.summary"].detail,
                   f"ledger.summary FAILs it too, got {unbalanced['ledger.summary']}")
        # The same discard in life 0, which a stop ended: its residual and its egress gap
        # leave the discards out, so neither reads them as loss.
        stopped = results("sources-discard-stopped", transforms=chain,
                          discard=[("parse", 50, 7)])
        egress = stopped["ledger.egress"].detail
        expect(stopped["ledger.egress"].status == checks.PASS
               and "life 0: W − D − B − X − ΣT_disc − V 0 = residual 60" in egress,
               f"a stopped life's egress gap leaves its counted discards out, got {egress}")
        expect(stopped["ledger.intake"].status == checks.PASS
               and "life 0: W − D − B − X − ΣT_disc − Ab 60 within [0, R]"
               in stopped["ledger.intake"].detail,
               f"a stopped life's residual leaves its counted discards out, got "
               f"{stopped['ledger.intake']}")

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
             "ledger.source 1: `wire` must be one of udp, tcp, file, http", "an unknown wire")
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

    # A route that can send one event to several sinks is refused under split = "route".
    def route_problems(text, split="route"):
        with tempfile.TemporaryDirectory() as tmp:
            scenario_path = Path(tmp) / "scenario.toml"
            changed = copy.deepcopy(raw)
            for key, name in changed["configs"].items():
                (Path(tmp) / name).write_text(
                    text if key == "sut" else (path.parent / name).read_text())
            changed["ledger"]["split"] = split
            first = changed["ledger"]["sink"][0]
            changed["ledger"]["sink"] = [dict(first, target="tier_a"),
                                         dict(first, id="other", target="tier_b")]
            try:
                scenario.from_dict(changed, scenario_path)
                return []
            except scenario.ScenarioError as err:
                return [p for p in err.problems if "route " in p and "several sinks" in p]

    single = "components:\n  split:\n    type: route\n    by: { attribute: shard }\n" \
             "    routes: { \"0\": tier_a, \"1\": tier_b }\n    sources: [window]\n"
    expect(route_problems(single) == [],
           f"a one-target-per-value route loads under split = route, got {route_problems(single)}")
    flow_list = "components:\n  split: { type: route, by: { attribute: shard }, " \
                "routes: { \"0\": [tier_a, tier_b] } }\n"
    block_list = "components:\n  split:\n    type: route\n    by: { attribute: shard }\n" \
                 "    routes:\n      \"0\":\n        - tier_a\n        - tier_b\n"
    for name, text in (("a flow list", flow_list), ("a block list", block_list)):
        problems = route_problems(text)
        expect(len(problems) == 1 and "route 'split'" in problems[0]
               and "use split = \"target\", split = \"copy\", or one target per value"
               in problems[0],
               f"{name} under split = route is refused, got {problems}")
        expect(route_problems(text, split="copy") == [],
               f"{name} loads under split = copy, got {route_problems(text, split='copy')}")

    by_signals = "components:\n  split:\n    type: route_signals\n" \
                 "    signals: { logs: tier_a, metrics: tier_b }\n"
    problems = route_problems(by_signals)
    expect(len(problems) == 1 and "route_signals 'split'" in problems[0],
           f"a route_signals under split = route is refused, got {problems}")
    copied = route_problems(by_signals, split="copy")
    expect(copied == [], f"a route_signals loads under split = copy, got {copied}")

    # Under split = "target" both routers load, each sink naming its own aggregate, and the
    # router's targets must be the sinks' targets, every one.
    def target_problems(text, edit=None):
        text = text.replace("components:\n", "components:\n  statsd: { type: statsd_in }\n", 1)
        with tempfile.TemporaryDirectory() as tmp:
            scenario_path = Path(tmp) / "scenario.toml"
            changed = copy.deepcopy(raw)
            for key, name in changed["configs"].items():
                (Path(tmp) / name).write_text(
                    text if key == "sut" else (path.parent / name).read_text())
            ledger = changed["ledger"]
            ledger["split"] = "target"
            ledger["sut_router"] = "split"
            ledger.pop("sut_aggregate")
            first = ledger["sink"][0]
            ledger["sink"] = [dict(first, target="tier_a", aggregate="window_a"),
                              dict(first, id="other", target="tier_b", aggregate="window_b",
                                   transforms=["count_b"])]
            if edit:
                edit(changed)
            try:
                loaded = scenario.from_dict(changed, scenario_path)
                return [], loaded
            except scenario.ScenarioError as err:
                return err.problems, None

    for name, text in (("a route_signals", by_signals), ("a flow list", flow_list),
                       ("a block list", block_list.replace("routes:\n", "routes:\n"
                                                           "      \"1\": tier_b\n"))):
        problems, loaded = target_problems(text)
        expect(problems == [] and loaded.split == "target"
               and [s["aggregate"] for s in loaded.sinks] == ["window_a", "window_b"],
               f"{name} loads under split = target, got {problems}")
    # A `targets:` line inside a script's source isn't the component's key.
    commented_script = ("  script:\n    type: lua\n    sources: [split]\n    source: |\n"
                        "      -- targets: tier_b\n      function process(e) return e end\n")
    problems, _ = target_problems(by_signals + commented_script)
    expect(not any("also fills" in problem for problem in problems),
           f"a commented targets line in a lua source isn't refused, got {problems}")
    partial = "components:\n  split:\n    type: route_signals\n" \
              "    signals: { logs: tier_a, metrics: tier_b, traces: tier_c }\n"
    for text, edit, needle, what in (
            (partial, None, "fills tier_c, which no [[ledger.sink]] follows",
             "a router target no sink follows"),
            (single.replace("tier_b", "tier_x"), None, "`target` tier_b isn't one route",
             "a sink target the router doesn't fill"),
            (by_signals + "  more:\n    type: route\n    by: { attribute: shard }\n"
             "    routes: { \"0\": [tier_c, tier_b] }\n",
             None, "route 'more' also fills a sink's target", "a second router"),
            (by_signals + "  more:\n    type: route_signals\n    signals: { logs: tier_a }\n",
             None, "route_signals 'more' also fills a sink's target",
             "a second route_signals"),
            (by_signals + "  script:\n    type: lua\n    targets:\n      - tier_b\n",
             None, "lua 'script' also fills a sink's target", "a lua with targets"),
            (by_signals + "  script: { type: lua_file, path: x.lua, targets: [tier_a] }\n",
             None, "lua_file 'script' also fills a sink's target", "a lua_file with targets"),
            (by_signals + "  script:\n    type: lua\n    targets:\n    - tier_b\n",
             None, "lua 'script' also fills a sink's target",
             "a lua whose targets list sits at the key's own indent"),
            (by_signals + commented_script + "    targets: [tier_a]\n",
             None, "lua 'script' also fills a sink's target",
             "a lua with a targets key after a commented one in its source"),
            (by_signals, lambda r: r["ledger"].update(sut_router="other"),
             "ledger.sut_router 'other' isn't a route or route_signals component",
             "a router the SUT config lacks"),
            (by_signals, lambda r: r["ledger"].pop("sut_router"),
             "ledger.split = \"target\" needs `sut_router`", "no router"),
            (by_signals, lambda r: r["ledger"].update(sut_aggregate="window"),
             "ledger.sut_aggregate is refused under split = \"target\"", "a shared aggregate"),
            (by_signals, lambda r: r["ledger"]["sink"][1].pop("aggregate"),
             "ledger.sink 2: under split = \"target\" a sink needs `aggregate`",
             "a sink with no aggregate"),
            (by_signals, lambda r: r["ledger"]["sink"][1].update(aggregate="window_a"),
             "ledger.sink 2: window_a is on another sink's branch",
             "two sinks sharing an aggregate"),
            (by_signals, lambda r: r["ledger"]["sink"][1].update(transforms="count_b"),
             "ledger.sink 2: `transforms` must list", "a transforms that isn't a list"),
            (by_signals, lambda r: r["ledger"].update(sut_script="lua"),
             "ledger.sut_script can't be combined with split = \"target\"", "a script"),
            (by_signals, lambda r: r["ledger"].update(vm_every_window=True),
             "ledger.vm_every_window can't be combined", "every window"),
            (by_signals, lambda r: r["ledger"]["sink"].pop(),
             "ledger.split = \"target\" needs two or more", "one sink")):
        problems, _ = target_problems(text, edit)
        expect(any(needle in problem for problem in problems),
               f"{what} under split = target is refused with {needle!r}, got {problems}")
    for edit, needle, what in (
            (lambda r: r["ledger"].update(sut_router="split"),
             "ledger.sut_router is for split = \"target\"", "a router under copy"),
            (lambda r: r["ledger"]["sink"][0].update(aggregate="window"),
             "ledger.sink 1: `aggregate` is for split = \"target\"", "an aggregate under copy")):
        changed = copy.deepcopy(raw)
        edit(changed)
        _refused(path, changed, needle, what)

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
           and record.get("affects_file_ingress") is False
           and record.get("affects_http_ingress") is True,
           f"the driver records every wire's flag, got {record}")

    tcp = scenario.load(root / "scenarios" / "tcp-ingress" / "scenario.toml")
    expect(tcp.sources[0].get("wire") == "tcp"
           and [step.action for step in tcp.steps] == [
               "netem", "netem", "netem", "pause", "stop", "kill", "partition", "pause"],
           f"tcp-ingress names a TCP source and its eight faults, got {tcp.sources}")


HTTP_SUT_CONFIGS = {
    "influxdb_in": "components:\n  in: { type: influxdb_in, bind: 0.0.0.0:8086 }\n",
    "otlp_in": "components:\n  in:\n    type: otlp_in\n    bind: 0.0.0.0:4318\n",
    "splunk_hec_in": "components:\n  in: { type: splunk_hec_in, bind: 0.0.0.0:8088 }\n",
    "datadog_in": "components:\n  in: { type: datadog_in, bind: 0.0.0.0:8080 }\n",
    "prometheus_in": "components:\n  in:\n    type: prometheus_in\n    bind: 0.0.0.0:9090\n",
}
# Each kind's request counter on its write path, as `_http_run` writes it: the name and the tags
# beside `class`.
HTTP_REQUESTS = {
    "influxdb_in": ("logit.input.requests", {"route": "write"}),
    "splunk_hec_in": ("logit.input.requests", {"route": "event"}),
    "datadog_in": ("logit.input.requests", {"route": "series_v2"}),
    "prometheus_in": ("logit.input.writes", {}),
}


def _http_run(tmp, name, kind="influxdb_in", sut_edit=None, gen_edit=None, stop_dropped=True,
              timeline_extra=(), **kwargs):
    """`_ledger_run`'s two-life run directory with its one source on an HTTP wire into a `kind`
    listener `in`, whose config the run's `configs/` holds: W is the listener's
    `events.sent`, so no datagram is counted and life 0 reads nothing it doesn't send; under
    a kind in `HTTP_REQUESTS` each drain's 10 batches are 10 `ok` write requests; G is the generator sink's
    `events.delivered`, its `events.received` beside it, and during the stop over `STOP` the
    sink has nothing acknowledged and, with `stop_dropped`, counts its overflow dropped; the stop
    is HTTP-affecting, as the driver records it. `sut_edit(record)` and `gen_edit(record)` may
    change or drop (return None) each SUT and generator record, or return a list of them."""
    kwargs.setdefault("life0_extra", 0)
    kwargs.setdefault("v0_extra", 0)
    run_dir = _ledger_run(tmp, name, shutdown_drops=0, timeline_extra=timeline_extra, **kwargs)

    def emit(out, record, edit):
        record = edit(record) if edit else record
        out += [json.dumps(one) for one in (record if isinstance(record, list) else [record])
                if one is not None]

    out = []
    for line in (run_dir / "logs" / "logit.stdout").read_text().splitlines():
        record = json.loads(line)
        metric = record["metrics"][0]
        if metric["name"] == "logit.input.datagrams":
            continue
        records = [record]
        if (metric["name"] == "logit.component.events.sent"
                and record["attributes"]["component"] == "in"):
            batches = metric["value"] // 100
            records.append(json.loads(_line(record["timestamp"], "logit.component.batches.sent",
                                            "sum", batches, component="in")))
            if kind in HTTP_REQUESTS:
                name, tags = HTTP_REQUESTS[kind]
                records.append(json.loads(_line(
                    record["timestamp"], name, "sum", batches, component="in",
                    attrs={**tags, "class": "ok"})))
        for one in records:
            emit(out, one, sut_edit)
    (run_dir / "logs" / "logit.stdout").write_text("\n".join(out) + "\n")
    gen = []
    stopped = (_stamp(T0 + STOP[0]), _stamp(T0 + STOP[1]))
    for line in (run_dir / "logs" / "generator.stdout").read_text().splitlines():
        record = json.loads(line)
        metric = record["metrics"][0]
        if metric["name"] == "logit.output.datagrams":
            continue
        records = [record]
        if metric["name"] == "logit.output.messages":
            metric["name"] = "logit.component.events.delivered"
            records.append(json.loads(_line(record["timestamp"],
                                            "logit.component.events.received", "sum",
                                            metric["value"], component="out")))
            if stopped[0] < record["timestamp"] <= stopped[1]:
                if stop_dropped:
                    records.append(json.loads(_line(
                        record["timestamp"], "logit.component.batches.dropped", "sum",
                        metric["value"] // 100, component="out",
                        attrs={"reason": "overflow_newest"})))
                metric["value"] = 0
        for one in records:
            emit(gen, one, gen_edit)
    (run_dir / "logs" / "generator.stdout").write_text("\n".join(gen) + "\n")
    timeline = []
    for line in (run_dir / "timeline.jsonl").read_text().splitlines():
        record = json.loads(line)
        if record.get("step") == "c0s1":
            record["affects_http_ingress"] = True
        timeline.append(json.dumps(record))
    (run_dir / "timeline.jsonl").write_text("\n".join(timeline) + "\n")
    (run_dir / "configs").mkdir(exist_ok=True)
    (run_dir / "configs" / "sut.yaml").write_text(HTTP_SUT_CONFIGS[kind])
    resolved = json.loads((run_dir / "scenario.resolved.json").read_text())
    resolved["configs"] = {"sut": "sut.yaml"}
    resolved["ledger"]["source"] = [{**resolved["ledger"]["source"][0], "wire": "http"}]
    (run_dir / "scenario.resolved.json").write_text(json.dumps(resolved))
    return run_dir


def _http_fault(step, action, on, start, end):
    """An apply and a revert of `action` on `on` that affects the HTTP wire alone."""
    flags = {"affects_udp_ingress": False, "affects_tcp_ingress": False,
             "affects_file_ingress": False, "affects_http_ingress": True}
    return (_action("apply", step, action, on, start, **flags),
            _action("revert", step, action, on, end - 1, end, **flags))


def _at_offset(name, offset, value, component, attrs=None, add=False):
    """A `sut_edit` or `gen_edit` that appends a `name` point of `value` under `component` to
    the drain at +`offset`, or with `add` adds `value` to the existing `name` point there."""
    stamp = _stamp(T0 + offset)

    def edit(record):
        metric = record["metrics"][0]
        if record["timestamp"] != stamp or record["attributes"].get("component") != component:
            return record
        if add and metric["name"] == name:
            metric["value"] += value
            return record
        if not add and metric["name"] in ("logit.component.events.sent",
                                          "logit.component.events.delivered"):
            return [record, json.loads(_line(stamp, name, "sum", value, component=component,
                                             attrs=attrs))]
        return record
    return edit


def _chain(*edits):
    """One record edit applying `edits` in order, each to every record the last returned."""
    def edit(record):
        records = [record]
        for one in edits:
            out = []
            for each in records:
                got = one(each)
                out += [r for r in (got if isinstance(got, list) else [got]) if r is not None]
            records = out
        return records
    return edit


def _http_ledger():
    """The ledger on an HTTP source: G from the writer's acknowledged events, W from the
    listener's `events.sent` less X, a stop judged, a judged window's surplus against the
    writer's transport-error attempts, `influxdb_in`'s request and deferral identities in
    `ledger.intake`, `otlp_in`'s rows with none, and the end-to-end bound in
    `ledger.summary`."""
    with tempfile.TemporaryDirectory() as tmp:
        def results(name, **kwargs):
            return {r.id: r for r in checks.run_all(_http_run(tmp, name, **kwargs))}

        clean = results("http-clean")
        for row in ("ledger.wire", "ledger.intake", "ledger.edge", "ledger.egress",
                    "ledger.summary", "recovery", "drops"):
            expect(clean[row].status == checks.PASS,
                   f"{row} PASSes the balanced HTTP fixture, got {clean[row]}")
        wire = clean["ledger.wire"].detail
        expect(wire.startswith("G 54,000 (acknowledged of 60,000 attempted) = W 54,000 − X 0 + "
                               "gap 0 (0 in reported windows, 0 in judged HTTP-affecting")
               and "overflow_newest 60" in wire,
               f"an HTTP source's G is acknowledged events, and its counted overflow stays out "
               f"of it, got {wire}")
        expect(any("judged (c0s1 stop on logit)" in line for line in clean["ledger.wire"].lines),
               f"a graceful stop is judged on HTTP, got {clean['ledger.wire'].lines}")
        intake = clean["ledger.intake"].detail
        expect("logit.input.requests{route=write} ok 340 + closed_consumer 0 == batches.sent "
               "340" in intake and "busy 0 == batches.deferred 0" in intake,
               f"ledger.intake shows influxdb_in's identities on the final life, got {intake}")
        expect("plus a resend allowance 0: sink: V 54,000 <= G − counted + resent 54,000"
               in clean["ledger.summary"].detail,
               f"ledger.summary bounds V end to end on HTTP, got {clean['ledger.summary'].detail}")

        # An acknowledged point the listener never sent: loss inside the judged stop FAILs.
        acked = results("http-stop-loss", gen_edit=_at_offset(
            "logit.component.events.delivered", 110, 1000, "out", add=True))
        expect(acked["ledger.wire"].status == checks.FAIL
               and acked["ledger.wire"].detail.startswith("1,000 lost outside reported HTTP "
                                                          "windows"),
               f"G above W inside a stop FAILs on HTTP, got {acked['ledger.wire']}")
        # A kill's window is reported, never judged.
        killed = results("http-kill-loss", sut_end="kill",
                         gen_edit=_at_offset("logit.component.events.delivered", 110, 1000, "out",
                                             add=True))
        expect(any("fault (c0s1 kill on logit)" in line for line in killed["ledger.wire"].lines)
               and killed["ledger.wire"].status == checks.PASS,
               f"a kill's window is reported on HTTP, got {killed['ledger.wire']}")

        # A judged window's surplus: each transport-error attempt allows one batch.
        late = _http_fault("x1", "netem", "logit", 200, 210)
        over = results("http-surplus", steady_surplus=350, timeline_extra=late)
        expect(over["ledger.wire"].status == checks.FAIL
               and "W − X exceeds G by 350, over the 200 its 0 transport-error attempt(s) and 2 "
               "edge(s) allow" in over["ledger.wire"].detail,
               f"a surplus past a judged HTTP window's edges FAILs, got {over['ledger.wire']}")
        resent = _at_offset("logit.output.requests", 230, 2, "out",
                            attrs={"class": "network_error"})
        allowed = results("http-surplus-resent", steady_surplus=350, timeline_extra=late,
                          gen_edit=resent)
        expect(allowed["ledger.wire"].status == checks.PASS
               and "surplus allowed 400 (2 transport-error attempt(s) + 2 edge(s), x one "
               "100-line batch)" in "\n".join(allowed["ledger.wire"].lines)
               and "2 transport-error attempt(s)" in allowed["ledger.wire"].detail,
               f"two transport-error attempts allow two batches, got {allowed['ledger.wire']}")
        # Only an attempt the listener may have taken allows a batch: fifteen timeouts
        # (`network_error`) allow fifteen, and fifteen refused dials (`connect_error`), which
        # sent nothing, allow none.
        timeouts = results("http-surplus-timeouts", steady_surplus=1650, timeline_extra=late,
                           gen_edit=_at_offset("logit.output.requests", 230, 15, "out",
                                               attrs={"class": "network_error"}))
        expect(timeouts["ledger.wire"].status == checks.PASS
               and "surplus allowed 1,700 (15 transport-error attempt(s) + 2 edge(s), x one "
               "100-line batch)" in "\n".join(timeouts["ledger.wire"].lines),
               f"fifteen timed-out attempts allow fifteen batches, got {timeouts['ledger.wire']}")
        dials = results("http-surplus-connects", steady_surplus=1650, timeline_extra=late,
                        gen_edit=_at_offset("logit.output.requests", 230, 15, "out",
                                            attrs={"class": "connect_error"}))
        expect(dials["ledger.wire"].status == checks.FAIL
               and "W − X exceeds G by 1,650, over the 200 its 0 transport-error attempt(s) and "
               "2 edge(s) allow" in dials["ledger.wire"].detail
               and "surplus allowed 200 (0 transport-error attempt(s) + 2 edge(s), x one "
               "100-line batch; 15 connect failure(s) allow none)"
               in "\n".join(dials["ledger.wire"].lines)
               and "0 transport-error attempt(s), 15 connect failure(s)"
               in dials["ledger.wire"].detail,
               f"fifteen refused dials allow no surplus, got {dials['ledger.wire']}")
        # A `503` isn't a transport error: it allows nothing.
        answered = _at_offset("logit.output.requests", 230, 2, "out", attrs={"class": "5xx"})
        refused = results("http-surplus-5xx", steady_surplus=350, timeline_extra=late,
                          gen_edit=answered)
        expect(refused["ledger.wire"].status == checks.FAIL
               and "2 answered 5xx and retried" in refused["ledger.wire"].detail,
               f"a 503 allows no surplus, got {refused['ledger.wire']}")

        # A batch refused with `503` closed_consumer is in W and X, not G: the wire nets it.
        refused_sent = _chain(
            _at_offset("logit.component.events.sent", 250, 300, "in", add=True),
            _at_offset("logit.component.events.dropped", 250, 300, "in",
                       attrs={"reason": "closed_consumer"}),
            _at_offset("logit.component.batches.sent", 250, 3, "in", add=True),
            _at_offset("logit.input.requests", 250, 3, "in",
                       attrs={"route": "write", "class": "closed_consumer"}))
        netted = results("http-closed-consumer", sut_edit=refused_sent, timeline_extra=late)
        expect(netted["ledger.wire"].status == checks.PASS
               and "W 54,300 − X 300 + gap 0" in netted["ledger.wire"].detail,
               f"an HTTP listener's X leaves the wire's W, got {netted['ledger.wire']}")
        expect("ok 340 + closed_consumer 3 == batches.sent 343" in netted["ledger.intake"].detail,
               f"a closed_consumer request is one batch sent, got {netted['ledger.intake']}")
        expect("G − counted + resent 54,000" in netted["ledger.summary"].detail,
               f"an HTTP listener's X isn't counted loss end to end, got "
               f"{netted['ledger.summary'].detail}")
        uncounted = results("http-closed-consumer-uncounted", timeline_extra=late,
                            sut_edit=_chain(
                                _at_offset("logit.component.events.sent", 250, 300, "in",
                                           add=True),
                                _at_offset("logit.component.batches.sent", 250, 3, "in",
                                           add=True)))
        expect(uncounted["ledger.wire"].status == checks.FAIL
               and uncounted["ledger.intake"].status == checks.FAIL
               and "ok 340 + closed_consumer 0 != batches.sent 343, so a request isn't one batch"
               in uncounted["ledger.intake"].detail,
               f"a batch sent with no request answered FAILs both rows, got "
               f"{uncounted['ledger.wire']}, {uncounted['ledger.intake']}")

        # A busy `503` is one deferred batch, and is no loss.
        busy = results("http-busy", sut_edit=_chain(
            _at_offset("logit.input.requests", 250, 3, "in",
                       attrs={"route": "write", "class": "busy"}),
            _at_offset("logit.input.batches.deferred", 250, 3, "in")))
        expect(busy["ledger.intake"].status == checks.PASS
               and "busy 3 == batches.deferred 3" in busy["ledger.intake"].detail
               and "busy 503s 3 and batches.deferred 3, a retry, not loss"
               in busy["ledger.wire"].detail
               and busy["drops"].status == checks.PASS,
               f"busy 503s are deferred batches, not loss, got {busy['ledger.intake']}, "
               f"{busy['ledger.wire']}")
        unmatched = results("http-busy-unmatched", sut_edit=_at_offset(
            "logit.input.requests", 250, 3, "in", attrs={"route": "write", "class": "busy"}))
        expect(unmatched["ledger.intake"].status == checks.FAIL
               and "busy 3 != batches.deferred 0, so a busy 503 isn't one deferred batch"
               in unmatched["ledger.intake"].detail,
               f"a busy 503 with no deferred batch FAILs, got {unmatched['ledger.intake']}")
        # The identities hold in every life a kill didn't end, not only the final one.
        earlier = results("http-busy-earlier", sut_edit=_at_offset(
            "logit.input.requests", 50, 3, "in", attrs={"route": "write", "class": "busy"}))
        expect(earlier["ledger.intake"].status == checks.FAIL
               and earlier["ledger.intake"].detail.startswith(
                   "life 0: busy 3 != batches.deferred 0, so a busy 503 isn't one deferred batch")
               and "life 0: logit.input.requests{route=write} ok 200 + closed_consumer 0 == "
               "batches.sent 200" in earlier["ledger.intake"].detail,
               f"an earlier life's busy 503 with no deferred batch FAILs, got "
               f"{earlier['ledger.intake']}")
        killed_busy = results("http-busy-killed", sut_end="kill", sut_edit=_at_offset(
            "logit.input.requests", 50, 3, "in", attrs={"route": "write", "class": "busy"}))
        expect(killed_busy["ledger.intake"].status == checks.PASS
               and "life 0 (killed): request identities not judged"
               in killed_busy["ledger.intake"].detail,
               f"a killed life's identities are reported unjudged, got "
               f"{killed_busy['ledger.intake']}")

        # V above G less counted loss, plus the resend allowance, FAILs end to end.
        surplus = results("http-v-surplus", v1_short=-300)
        expect(surplus["ledger.summary"].status == checks.FAIL
               and "V 54,300 > G − counted + resent 54,000, a surplus of 300"
               in surplus["ledger.summary"].detail,
               f"V over G end to end FAILs on HTTP, got {surplus['ledger.summary']}")
        covered = results("http-v-resent", v1_short=-300, gen_edit=_at_offset(
            "logit.output.requests", 230, 3, "out", attrs={"class": "network_error"}))
        expect("V 54,300 <= G − counted + resent 54,300" in covered["ledger.summary"].detail,
               f"three transport-error attempts cover 300 end to end, got "
               f"{covered['ledger.summary'].detail}")

        # A timed-out acknowledgment wait resends a batch the listener answered: each timed-out
        # id allows one batch, like a transport-error attempt, on the wire and end to end.
        acked_late = results("http-surplus-ack-timeouts", steady_surplus=350,
                             timeline_extra=late, gen_edit=_at_offset(
                                 "logit.output.acks", 230, 2, "out", attrs={"result": "timeout"}))
        expect(acked_late["ledger.wire"].status == checks.PASS
               and "surplus allowed 400 (2 transport-error attempt(s) or ack timeout(s) (2 ack "
               "timeout(s)) + 2 edge(s), x one 100-line batch)"
               in "\n".join(acked_late["ledger.wire"].lines)
               and "0 transport-error attempt(s), 2 ack timeout(s), 0 connect failure(s)"
               in acked_late["ledger.wire"].detail,
               f"two ack timeouts allow two batches, got {acked_late['ledger.wire']}")
        acked_v = results("http-v-ack-timeouts", v1_short=-300, gen_edit=_at_offset(
            "logit.output.acks", 230, 3, "out", attrs={"result": "timeout"}))
        expect("V 54,300 <= G − counted + resent 54,300" in acked_v["ledger.summary"].detail,
               f"three ack timeouts cover 300 end to end, got {acked_v['ledger.summary'].detail}")
        # A poll of `/ack` that fails in transport sends no data and allows nothing.
        polled = results("http-surplus-ack-polls", steady_surplus=350, timeline_extra=late,
                         gen_edit=_at_offset("logit.output.requests", 230, 2, "out",
                                             attrs={"route": "ack",
                                                    "class": "network_error"}))
        expect(polled["ledger.wire"].status == checks.FAIL
               and "W − X exceeds G by 350, over the 200 its 0 transport-error attempt(s) and 2 "
               "edge(s) allow" in polled["ledger.wire"].detail,
               f"a failed ack poll allows no surplus, got {polled['ledger.wire']}")

        # Each request in flight is a batch the listener counted before the writer read its
        # answer, so each edge of a judged run allows the batches the writer's
        # `logit.output.in_flight` read at the generator drains straddling it, at least one.
        def concurrent(name, reading, **kwargs):
            def gauge(record):
                if (record["metrics"][0]["name"] != "logit.component.events.delivered"
                        or record["attributes"].get("component") != "out"):
                    return record
                offset = telemetry.parse_rfc3339(record["timestamp"]) - T0
                return [record, json.loads(_line(record["timestamp"], "logit.output.in_flight",
                                                 "gauge", float(reading(offset)),
                                                 component="out"))]
            # Each fixture's writer is at `concurrency: 8`, so a rule read from the config
            # rather than the gauge widens its edges.
            run_dir = _http_run(tmp, name, gen_edit=gauge, **kwargs)
            (run_dir / "configs" / "generator.yaml").write_text(
                "components:\n  out:\n    type: splunk_hec_out\n    concurrency: 8\n")
            resolved = json.loads((run_dir / "scenario.resolved.json").read_text())
            resolved["configs"]["generator"] = "generator.yaml"
            (run_dir / "scenario.resolved.json").write_text(json.dumps(resolved))
            return {r.id: r for r in checks.run_all(run_dir)}

        before_kill = concurrent(
            "http-surplus-in-flight", lambda offset: 8 if 95 <= offset <= 105 else 1,
            sut_end="kill", timeline_extra=_http_fault("x1", "netem", "logit", 60, 101),
            sut_edit=_at_offset("logit.component.events.sent", 95, 350, "in", add=True))
        expect(before_kill["ledger.wire"].status == checks.PASS
               and "surplus allowed 900 (0 transport-error attempt(s) + 2 edge(s) holding 1 + 8 "
               "batch(es) read in flight, x one 100-line batch)"
               in "\n".join(before_kill["ledger.wire"].lines),
               f"eight requests in flight at a judged edge allow eight batches, got "
               f"{before_kill['ledger.wire']}")
        # Lag that drifts up by 7 batches across a judged run, its neighbour drifting on so
        # that no pair nets, is held by the 7 requests read in flight at its trailing edge.
        drifted = concurrent(
            "http-surplus-in-flight-drift",
            lambda offset: 7 if 195 <= offset <= 205 or 250 <= offset <= 260 else 1,
            timeline_extra=late, sut_edit=_chain(
                _at_offset("logit.component.events.sent", 205, 300, "in", add=True),
                _at_offset("logit.component.events.sent", 250, 400, "in", add=True),
                _at_offset("logit.component.events.sent", 270, -700, "in", add=True),
                _at_offset("logit.component.events.sent", 290, 700, "in", add=True)))
        expect(drifted["ledger.wire"].status == checks.PASS
               and "surplus allowed 800 (0 transport-error attempt(s) + 2 edge(s) holding 1 + 7 "
               "batch(es) read in flight, x one 100-line batch)"
               in "\n".join(drifted["ledger.wire"].lines),
               f"lag up to the requests read in flight at the trailing edge passes, got "
               f"{drifted['ledger.wire']}")
        # A run's lag surplus is its lag at the end less its lag at the start, so the leading
        # edge's reading backs none: 1,500 unexplained, with 8 read in flight at both edges and
        # no resend, FAILs against the 900 the trailing edge and the leading edge's batch allow.
        unexplained = concurrent(
            "http-surplus-in-flight-unexplained",
            lambda offset: 8 if 195 <= offset <= 205 or 250 <= offset <= 260 else 1,
            timeline_extra=late, sut_edit=_at_offset("logit.component.events.sent", 230, 1500,
                                                     "in", add=True))
        expect(unexplained["ledger.wire"].status == checks.FAIL
               and "W − X exceeds G by 1,500, over the 900 its 0 transport-error attempt(s) and "
               "2 edge(s) holding 1 + 8 batch(es) read in flight allow"
               in unexplained["ledger.wire"].detail,
               f"the leading edge's reading allows no surplus, got {unexplained['ledger.wire']}")
        # With none read in flight at its edges, a loss of 300 beside 1,000 duplicates in one
        # judged run FAILs: each edge keeps its one batch.
        masked = concurrent(
            "http-surplus-in-flight-masks", lambda offset: 0, timeline_extra=late,
            sut_edit=_chain(
                _at_offset("logit.component.events.sent", 205, -300, "in", add=True),
                _at_offset("logit.component.events.sent", 250, 1000, "in", add=True)))
        expect(masked["ledger.wire"].status == checks.FAIL
               and "W − X exceeds G by 700, over the 200 its 0 transport-error attempt(s) and 2 "
               "edge(s) allow" in masked["ledger.wire"].detail,
               f"no request read in flight widens no edge, got {masked['ledger.wire']}")

        # `splunk_hec_in` and `datadog_in` hold their write route's requests to batches as
        # `influxdb_in` does, each on its own route; `prometheus_in`'s receiver counts
        # `logit.input.writes{class}` with no route tag, and never answers busy.
        for kind, route_text in (
                ("splunk_hec_in", "logit.input.requests{route=event}"),
                ("datadog_in", "logit.input.requests{route=series_v2}"),
                ("prometheus_in", "logit.input.writes")):
            got = results(f"http-{kind}", kind=kind)
            intake = got["ledger.intake"].detail
            expect(got["ledger.wire"].status == checks.PASS
                   and got["ledger.intake"].status == checks.PASS
                   and f"final life 1: {route_text} ok 340 + closed_consumer 0 == batches.sent "
                   "340" in intake
                   and ("busy" in intake) == (kind != "prometheus_in"),
                   f"{kind}'s intake judges its request identities, got {got['ledger.intake']}")
            name, tags = HTTP_REQUESTS[kind]
            unanswered = results(f"http-{kind}-unanswered", kind=kind, sut_edit=_chain(
                _at_offset("logit.component.events.sent", 250, 300, "in", add=True),
                _at_offset("logit.component.batches.sent", 250, 3, "in", add=True),
                _at_offset("logit.component.events.dropped", 250, 300, "in",
                           attrs={"reason": "closed_consumer"})))
            expect(unanswered["ledger.intake"].status == checks.FAIL
                   and f"{route_text} ok 340 + closed_consumer 0 != batches.sent 343"
                   in unanswered["ledger.intake"].detail,
                   f"{kind}: a batch sent with no request counted FAILs, got "
                   f"{unanswered['ledger.intake']}")
            refused = results(f"http-{kind}-refused", kind=kind, sut_edit=_chain(
                _at_offset("logit.component.events.sent", 250, 300, "in", add=True),
                _at_offset("logit.component.batches.sent", 250, 3, "in", add=True),
                _at_offset("logit.component.events.dropped", 250, 300, "in",
                           attrs={"reason": "closed_consumer"}),
                _at_offset(name, 250, 3, "in", attrs={**tags, "class": "closed_consumer"})))
            expect(refused["ledger.intake"].status == checks.PASS
                   and f"{route_text} ok 340 + closed_consumer 3 == batches.sent 343"
                   in refused["ledger.intake"].detail,
                   f"{kind}: a closed_consumer request is one batch sent, got "
                   f"{refused['ledger.intake']}")

        # `otlp_in` counts no requests: its intake has no identity of its own, and its wire
        # still FAILs uncounted loss.
        otlp = results("http-otlp", kind="otlp_in")
        expect(otlp["ledger.wire"].status == checks.PASS
               and otlp["ledger.intake"].status == checks.PASS
               and "otlp_in counts no requests: W − D == E + B alone"
               in otlp["ledger.intake"].detail,
               f"otlp_in's rows PASS its balanced fixture, got {otlp['ledger.wire']}, "
               f"{otlp['ledger.intake']}")
        otlp_lost = results("http-otlp-lost", kind="otlp_in", gen_edit=_at_offset(
            "logit.component.events.delivered", 60, 1000, "out", add=True))
        expect(otlp_lost["ledger.wire"].status == checks.FAIL
               and otlp_lost["ledger.wire"].detail.startswith("1,000 lost outside reported HTTP"),
               f"acknowledged points otlp_in never sent FAIL, got {otlp_lost['ledger.wire']}")
        otlp_over = results("http-otlp-surplus", kind="otlp_in", steady_surplus=350,
                            timeline_extra=late)
        expect(otlp_over["ledger.wire"].status == checks.FAIL,
               f"a surplus past otlp_in's allowance FAILs, got {otlp_over['ledger.wire']}")

        # A config the checks can't read leaves the listener's kind unknown, reported.
        blind_dir = _http_run(tmp, "http-no-config")
        (blind_dir / "configs" / "sut.yaml").unlink()
        blind = {r.id: r for r in checks.run_all(blind_dir)}
        expect("an http listener whose kind the SUT config doesn't give"
               in blind["ledger.intake"].detail,
               f"an unknown HTTP listener kind is reported, got {blind['ledger.intake']}")


def _http_rules(root):
    """The `http` wire's load-time cross-checks against the SUT and generator configs, its fault
    predicates and judgments, and the shipped http-ingress scenario."""
    path, raw = _base(root)

    def with_configs(sut, generator, wire, listener="statsd", sink="statsd"):
        """`load()`'s problems for statsd-vm with its SUT and generator configs replaced and its
        source's `wire`, `sut_listener`, and `generator_sink` set."""
        with tempfile.TemporaryDirectory() as tmp:
            changed = copy.deepcopy(raw)
            texts = {"sut": sut, "generator": generator}
            for key, name in changed["configs"].items():
                (Path(tmp) / name).write_text(texts[key])
            changed["ledger"]["source"][0].update(sut_listener=listener, generator_sink=sink)
            if wire is not None:
                changed["ledger"]["source"][0]["wire"] = wire
            try:
                scenario.from_dict(changed, Path(tmp) / "scenario.toml")
                return []
            except scenario.ScenarioError as err:
                return err.problems

    influx_in = "components:\n  influx: { type: influxdb_in, bind: 0.0.0.0:8086 }\n"
    otlp_in = "components:\n  otlp:\n    type: otlp_in\n    protocol: grpc\n"
    influx_out = ("components:\n  load: { type: generate_in }\n  influx:\n    type: influxdb_out\n"
                  "    sources: [load]\n")
    otlp_out = "components:\n  otlp: { type: otlp_out, endpoint: http://logit:4318 }\n"
    statsd_out = "components:\n  statsd: { type: statsd_out, endpoint: logit:8125 }\n"
    statsd_in = "components:\n  statsd: { type: statsd_in, bind: 0.0.0.0:8125 }\n"
    loaded = with_configs(influx_in, influx_out, "http", "influx", "influx")
    expect(loaded == [], f"an http source from influxdb_out into influxdb_in loads, got {loaded}")
    loaded = with_configs(otlp_in, otlp_out, "http", "otlp", "otlp")
    expect(loaded == [], f"an http source from otlp_out into otlp_in loads, got {loaded}")
    problems = with_configs(influx_in, influx_out, None, "influx", "influx")
    expect(any("`wire` is udp, but `sut_listener` 'influx' is a influxdb_in with no transport:, "
               "an http listener" in p for p in problems),
           f"influxdb_in with no wire named is refused, got {problems}")
    problems = with_configs(influx_in, statsd_out, "http", "influx", "statsd")
    expect(problems == ["ledger.source 1: an http wire's `generator_sink` must be one of "
                        "influxdb_out, otlp_out, splunk_hec_out, datadog_out, prometheus_out, "
                        "and 'statsd' is a statsd_out"],
           f"an http wire's writer must be an HTTP sink, got {problems}")
    problems = with_configs(statsd_in, influx_out, None, "statsd", "influx")
    expect(problems == ["ledger.source 1: `generator_sink` 'influx' is an influxdb_out, which "
                        "only an http wire reads"],
           f"an HTTP sink on a UDP wire is refused, got {problems}")
    problems = with_configs(statsd_in, statsd_out, "http")
    expect(any("`wire` is http, but `sut_listener` 'statsd' is a statsd_in with no transport:, "
               "a udp listener" in p for p in problems),
           f"an http source on a UDP listener is refused, got {problems}")

    # Three more listeners, each with its writer. `prometheus_in` has no `transport:`: its mode
    # gives its wire, `bind:` the remote-write receiver, an http listener, and
    # `scrape_targets:` a scrape listener (`_scrape_rules`).
    for listener, writer in (
            ("splunk: { type: splunk_hec_in, bind: 0.0.0.0:8088 }",
             "splunk: { type: splunk_hec_out, endpoint: http://logit:8088/services/collector }"),
            ("splunk: { type: datadog_in, bind: 0.0.0.0:8080 }",
             "splunk: { type: datadog_out, api_key: k }"),
            ("splunk:\n    type: prometheus_in\n    bind_auth: {}\n    bind: 0.0.0.0:9090",
             "splunk: { type: prometheus_out, endpoint: http://logit:9090/api/v1/write }")):
        loaded = with_configs(f"components:\n  {listener}\n", f"components:\n  {writer}\n",
                              "http", "splunk", "splunk")
        expect(loaded == [], f"an http source from {writer} into {listener} loads, got {loaded}")
    scrape = ("components:\n  prom:\n    type: prometheus_in\n    bind_tls: {}\n"
              "    scrape_targets: [{ url: http://generator:9464/metrics }]\n")
    prom_out = "components:\n  prom: { type: prometheus_out, endpoint: http://x/api/v1/write }\n"
    problems = with_configs(scrape, prom_out, "http", "prom", "prom")
    expect(problems == ["ledger.source 1: `wire` is http, but `sut_listener` 'prom' is a "
                        "prometheus_in on scrape_targets:, a scrape listener"],
           f"an http source on a scraping prometheus_in is refused, got {problems}")
    problems = with_configs("components:\n  prom: { type: prometheus_in, bind: 0.0.0.0:9090 }\n",
                            prom_out, None, "prom", "prom")
    expect(any("`wire` is udp, but `sut_listener` 'prom' is a prometheus_in on bind:, an http "
               "listener" in p for p in problems),
           f"prometheus_in on bind: with no wire named is refused, got {problems}")
    problems = with_configs("components:\n  prom: { type: prometheus_in, bind_tls: {} }\n",
                            prom_out, "http", "prom", "prom")
    expect(problems == ["ledger.source 1: `sut_listener` 'prom' is a prometheus_in with no "
                        "mode's key set, which the ledger has no wire for"],
           f"prometheus_in with neither mode's key has no wire, got {problems}")

    roles = scenario.BASE_ROLES
    for action, on, http in (
            ("netem", "generator", True), ("netem", "logit", True),
            ("stop", "logit", True), ("restart", "logit", True), ("kill", "logit", True),
            ("partition", "generator", True), ("pause", "generator", True),
            ("stop", "victoria-metrics", False), ("netem", "victoria-metrics", False),
            ("mark", "logit", True), ("respond", "standin", False),
            ("signal", "logit", False)):
        step = scenario.Step("c0s1", 0, 0, action, on, "", 0, 1)
        got = faults.ACTIONS[action].affects("http", step, roles)
        expect(got == http, f"{action} on {on} affects http {got}, expected {http}")
    for action, how in (("kill", "fault"), ("exit", "fault"), ("stop", "judged"),
                        ("restart", "judged"), ("netem", "judged"), ("partition", "judged"),
                        ("pause", "judged")):
        fault = checks.Fault("x", action, "logit", 0, 1, affects_udp_ingress=True,
                             affects_egress=False, affects_http_ingress=True)
        expect(checks.wire_judgment(fault, "http") == how,
               f"a {action} on logit is {checks.wire_judgment(fault, 'http')} on HTTP, "
               f"expected {how}")

    http = scenario.load(root / "scenarios" / "http-ingress" / "scenario.toml")
    expect(http.sources[0].get("wire") == "http"
           and http.sources[0].get("sut_transforms") == ["count"]
           and [step.action for step in http.steps] == [
               "netem", "netem", "netem", "stop", "stop", "restart", "kill"]
           and [e.name for e in http.expectations] == [
               "listener-defers", "generator-retries-503s", "generator-resends",
               "generator-dials-time-out", "sink-never-drops"],
           f"http-ingress names an HTTP source, its seven faults, and its expects, got "
           f"{http.sources}")
    for name, transforms in (("splunk-hec-ingress", ["count"]), ("datadog-ingress", None),
                             ("rw-ingress", ["count"])):
        one = scenario.load(root / "scenarios" / name / "scenario.toml")
        expect(one.sources[0].get("wire") == "http"
               and one.sources[0].get("sut_transforms") == transforms
               and [step.action for step in one.steps] == [
                   "netem", "netem", "netem", "stop", "stop", "restart", "kill"],
               f"{name} names an HTTP source and its seven faults, got {one.sources}")


def _as_file_wire(run_dir, w_minus_g=0, counted=True, line_bytes=95, files_open=None,
                  rotations=None, max_files=None):
    """Turns a ledger fixture into a file-wire run in place: the listener's datagrams become
    `logit.input.lines`, the generator's `output.messages` its `file_out`'s
    `logit.component.events.delivered`, set so the run's W − G is `w_minus_g`, and the source
    names `wire = "file"`. `counted` records the driver's `shared_counted` phase with G's lines
    at `line_bytes` each (a callable of G, or a dict, replaces it). `files_open` is
    `{offset: n}` of `logit.input.files.open` gauges written for the listener `in`.
    `rotations` is `{offset: n}` of the writer's `logit.output.file.rotations`, and `max_files`
    writes a generator config whose `file_out` `out` keeps that many files."""
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
    for offset, n in (rotations or {}).items():
        gen.append(_line(_stamp(T0 + offset), "logit.output.file.rotations", "sum", n,
                         component="out"))
    gen_path.write_text("\n".join(gen) + "\n")
    if counted:
        phase = (counted(g) if callable(counted) else counted if isinstance(counted, dict)
                 else {"files": 3, "lines": g, "bytes": g * line_bytes})
        with open(run_dir / "timeline.jsonl", "a") as timeline:
            timeline.write(json.dumps(_phase("shared_counted", 301, **phase)) + "\n")
            timeline.write(json.dumps(_phase("tail_caught_up", 302, reached=True, w=w, g=g,
                                             reads=2)) + "\n")
    resolved_path = run_dir / "scenario.resolved.json"
    resolved = json.loads(resolved_path.read_text())
    resolved["ledger"]["source"] = [{**resolved["ledger"]["source"][0], "wire": "file"}]
    if max_files is not None:
        (run_dir / "configs").mkdir(exist_ok=True)
        (run_dir / "configs" / "logit-generator.yaml").write_text(
            "components:\n  out:\n    type: file_out\n    path: /shared/app.log\n"
            "    # a comment's max_files: 99 is ignored\n"
            f"    rotate: {{ max_bytes: 4MiB, max_files: {max_files} }}\n")
        resolved["configs"] = {**resolved.get("configs", {}),
                               "generator": "logit-generator.yaml"}
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
        # A node between the tailer and `window` discards 7 lines, counted: the end-to-end
        # bound counts them with the rest of the counted loss, so V 5 above its share FAILs.
        def discarding(name, surplus=0):
            run_dir = _ledger_run(tmp, name, shutdown_drops=0, life0_extra=30)
            _discard(_with_chain(run_dir, ("fresh",)), ("fresh",), "fresh", 200, 7)
            path = run_dir / "vm-export" / "sink.jsonl"
            series = vm.parse_export(path.read_text())
            series[0]["values"][-1] += surplus
            _write_export(run_dir, "sink", series)
            return {r.id: r for r in checks.run_all(_as_file_wire(run_dir))}

        fresh = discarding("file-discard")
        expect(fresh["ledger.summary"].status == checks.PASS
               and "G 54,030 less counted loss 7" in fresh["ledger.summary"].detail,
               f"the end-to-end bound counts the path's discards, got {fresh['ledger.summary']}")
        over = discarding("file-discard-surplus", surplus=5)["ledger.summary"]
        expect(over.status == checks.FAIL
               and "> G − counted + replay 54,024, a surplus of 4" in over.detail,
               f"V above G less the counted discards FAILs the end-to-end bound, got {over}")
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

        # A writer keeping eight files rotated ten times outside the stop, so it deleted three:
        # the shared directory holds eight files and fewer lines than G, and G stands alone.
        steady = {offset: 1 for offset in (20, 40, 60, 80, 160, 180, 200, 220, 240, 260)}

        def deleting(lines_short=500, files=8):
            return lambda g: {"files": files, "lines": g - lines_short,
                              "bytes": (g - lines_short) * 95}

        deleted = stopped("file-deleted", rotations=steady, max_files=8, counted=deleting())
        detail = deleted["ledger.wire"].detail
        expect(deleted["ledger.wire"].status == checks.PASS
               and "53,530 lines in 8 files, the writer having deleted 3" in detail
               and any(line.startswith("the writer deleted 3 file(s); G stands alone, 500 "
                                       "line(s) above the shared directory's count, against "
                                       "about 132,452 in 3 file(s)")
                       for line in deleted["ledger.wire"].lines),
               f"a writer that deleted files leaves G alone, got {deleted['ledger.wire']} "
               f"{deleted['ledger.wire'].lines}")
        short = stopped("file-short-undeleted", rotations={20: 1, 40: 1, 60: 1}, max_files=8,
                        counted=deleting(files=4))
        expect(short["ledger.wire"].status == checks.FAIL
               and "the shared directory holds 53,530 line(s) in 4 file(s), but the writer's "
               "events.delivered is 54,030" in short["ledger.wire"].detail,
               f"a short count with no deletion FAILs, got {short['ledger.wire']}")
        above = stopped("file-deleted-above", rotations=steady, max_files=8,
                        counted=deleting(lines_short=-1))
        expect(above["ledger.wire"].status == checks.FAIL
               and above["ledger.wire"].detail.startswith(
                   "precondition: the shared directory holds 54,031 line(s), more than the "
                   "writer's events.delivered 54,030"),
               f"a count above G FAILs, got {above['ledger.wire']}")
        wrong = stopped("file-deleted-files", rotations=steady, max_files=8,
                        counted=deleting(files=7))
        expect(wrong["ledger.wire"].status == checks.FAIL
               and wrong["ledger.wire"].detail.startswith(
                   "precondition: the writer's rotations leave 8 file(s) in the shared "
                   "directory, and it holds 7"),
               f"a file count off the writer's retention FAILs, got {wrong['ledger.wire']}")
        # Six rotations inside the stop at +101 to +131 delete, with eight files kept, a file a
        # tailer one file behind hadn't finished; five don't trip the guard.
        down = {offset: 1 for offset in (105, 110, 115, 120, 125, 130)}
        tripped = stopped("file-retention", rotations=down, max_files=8)
        expect(tripped["ledger.wire"].status == checks.FAIL
               and tripped["ledger.wire"].detail.startswith(
                   "the writer rotated 6 time(s) while the SUT was down (stop [+101s, +131s]), "
                   "enough to delete a file it hadn't read: out keeps 8 files, so 6 "
                   "rotation(s) in a stop trip the guard"),
               f"a SUT down for max_files − 2 rotations FAILs, got {tripped['ledger.wire']}")
        del down[130]
        under = stopped("file-retention-under", rotations=down, max_files=8)
        expect(under["ledger.wire"].status == checks.PASS,
               f"five rotations while the SUT is down pass, got {under['ledger.wire']}")

        # A pause keeps the tailer's open files, so only a file created after it began can be
        # deleted unread, at its ninth rotation with eight files kept: eight trip the guard,
        # seven, which would trip it in a stop, don't.
        pause = [_action("apply", "c0s2", "pause", "logit", 200, 201),
                 _action("revert", "c0s2", "pause", "logit", 260, 260)]

        def paused(name, rotations, **kwargs):
            run_dir = _ledger_run(tmp, name, shutdown_drops=0, life0_extra=30,
                                  timeline_extra=pause)
            return {r.id: r for r in checks.run_all(_as_file_wire(
                run_dir, rotations={o: 1 for o in range(205, 205 + 5 * rotations, 5)},
                max_files=8, **kwargs))}

        long_pause = paused("file-retention-pause", 8, counted=deleting(files=8))
        expect(long_pause["ledger.wire"].status == checks.FAIL
               and long_pause["ledger.wire"].detail.startswith(
                   "the writer rotated 8 time(s) while the SUT was down (pause [+200s, +260s]), "
                   "enough to delete a file it hadn't read: out keeps 8 files, so 8 "
                   "rotation(s) in a pause trip the guard"),
               f"a pause over max_files rotations FAILs, got {long_pause['ledger.wire']}")
        short_pause = paused("file-retention-pause-under", 7)
        expect(short_pause["ledger.wire"].status == checks.PASS,
               f"seven rotations in a pause pass, got {short_pause['ledger.wire']}")

        # A killed life at 200 lines/s, 5 s drains, and a 5 s checkpoint_interval: below, 1,000
        # lost counts; above, 1,000 lines plus two files x 690 lines a 64 KiB read holds at 95
        # bytes a line; the stopped life adds one split.
        def killed(name, w_minus_g, no_drains=None):
            run_dir = _kill_run(tmp, name, every_window=False, no_drains=no_drains)
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
        # V holds the replayed lines too, so it exceeds G less counted loss by up to the band,
        # and the bound's label names the band.
        expect(edge["ledger.summary"].status == checks.PASS
               and "plus the replay band's 2,381" in edge["ledger.summary"].detail
               and "<= G − counted + replay " in edge["ledger.summary"].detail,
               f"the end-to-end bound adds the band's upper edge, got "
               f"{edge['ledger.summary'].detail}")

        # A killed life with one drain, at +200, has no drain interval to take a rate from:
        # all of that drain's 1,000 lines can replay, plus the two files' reads.
        young = killed("file-kill-one-drain", 2_381, no_drains=(134, 200))
        expect(young["ledger.wire"].status == checks.PASS
               and "against the replay band [-1,000, 2,381]" in young["ledger.wire"].detail
               and any("life 1 (killed): [-1,000, +2,380]: one drain, so its W of 1,000 lines "
                       "can all replay" in line for line in young["ledger.wire"].lines),
               f"a killed life with one drain may replay all of its W, got "
               f"{young['ledger.wire']} {young['ledger.wire'].lines}")
        young_over = killed("file-kill-one-drain-over", 2_382, no_drains=(134, 200))
        expect(young_over["ledger.wire"].status == checks.FAIL,
               f"a replay past a one-drain life's W FAILs, got {young_over['ledger.wire']}")
        # A checkpoint after that drain can commit up to about one drain's reads the kill
        # never exported: a shortfall of the drain's W passes, one line more FAILs.
        young_short = killed("file-kill-one-drain-short", -1_000, no_drains=(134, 200))
        expect(young_short["ledger.wire"].status == checks.PASS,
               f"a one-drain life may fall short by its W, got {young_short['ledger.wire']}")
        young_lost = killed("file-kill-one-drain-lost", -1_001, no_drains=(134, 200))
        expect(young_lost["ledger.wire"].status == checks.FAIL,
               f"a shortfall past a one-drain life's W FAILs, got {young_lost['ledger.wire']}")

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
    stale = scenario.load(root / "scenarios" / "tail-rotate-stale" / "scenario.toml")
    expect([source.get("sut_transforms") for source in stale.file_sources] == [
               ["parse", "stamp", "labels", "count", "metrics_only", "fresh"]],
           f"tail-rotate-stale lists its discarding node last, got {stale.file_sources}")
    delete = scenario.load(root / "scenarios" / "tail-rotate-delete" / "scenario.toml")
    expect([source["wire"] for source in delete.file_sources] == ["file"]
           and delete.max_duration is None and delete.duration == 4 * 3600
           and [e.name for e in delete.expectations] == [
               "files-rotated", "no-gone-bytes", "no-checkpoint-errors", "files-open-bounded",
               "fds-bounded", "files-open-plateau", "writer-deletes", "watches-bounded"],
           f"tail-rotate-delete is a file wire with no max_duration and its expects, got "
           f"{delete.file_sources} {delete.max_duration}")
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

        # The wait: W read from captured chunks reaches G, the writer's events.delivered read
        # from its own chunk, or stops growing. The writer deleted a file of two lines, so the
        # shared directory holds 10 of its 12, and W at 10 hasn't caught up.
        (shared / "app.log").write_text("x\n" * 10)
        logs = run.run_dir / "logs"

        class Capture:
            def __init__(self, deltas):
                self.deltas = list(deltas)
                self.offset = 0
                self.writer_reads = 0

            def chunk(self, ids):
                self.offset += 5
                stamp = _stamp(T0 + self.offset)
                if "generator" in ids:
                    self.writer_reads += 1
                    with open(logs / "generator.stdout", "a") as out:
                        out.write(_line(stamp, "logit.process.uptime", "gauge",
                                        float(self.offset)) + "\n")
                        out.write(_line(stamp, "logit.component.events.delivered", "sum", 12,
                                        component="file") + "\n")
                    return
                delta = self.deltas.pop(0) if self.deltas else 0
                with open(logs / "logit.stdout", "a") as out:
                    out.write(_line(stamp, "logit.process.uptime", "gauge",
                                    float(self.offset)) + "\n")
                    out.write(_line(stamp, "logit.input.lines", "sum", delta,
                                    component="tail") + "\n")

        clock = [1000.0]
        reals = (driver._now, driver.time.sleep)
        driver._now = lambda: clock[0]
        driver.time.sleep = lambda s: clock.__setitem__(0, clock[0] + s)
        try:
            run.ids = {"logit": "a", "generator": "b"}
            run.t0 = clock[0]
            run.timeline = _Records()
            run.capture = Capture([4, 4, 2, 2])
            began = clock[0]
            run.wait_tailed()
            reached = run.timeline[-1]
            writer_reads = run.capture.writer_reads
            for name in ("logit.stdout", "generator.stdout"):
                (logs / name).write_text("")
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
               and reached["w"] == 12 and reached["g"] == 12 and reached["counted_lines"] == 10
               and reached["reads"] == 4 and writer_reads == 1,
               f"the wait ends once W reaches G, not the shared count, got {reached}")
        expect(held["reached"] is False and held.get("held") is True and held["w"] == 7
               and held["g"] == 12,
               f"the wait ends once W holds short of it, got {held}")
        expect(clock[0] - began >= collect.LOG_LAG_NS / 1e9,
               "the wait reads G from a writer chunk at least a log lag after it began")


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
            seq.run_dir = driver.claim_run_dir(seq.out_dir)
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
TWO_SUT_HOP_CONFIG = "  hop: { type: logit_out, endpoint: collector:7400, concurrency: 8 }\n"
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
               "allowance 100: sink: V 60,100 <= G − counted + partial 60,100"
               in partial["ledger.summary"].detail,
               f"one ambiguous drop on a TCP source allows one batch of surplus end to end, "
               f"got {partial['ledger.summary']}")
        beyond_partial = tcp_hop("two-tcp-partial-beyond", 101, ambiguous_at=150)
        expect(beyond_partial["ledger.summary"].status == checks.FAIL
               and "sink: V 60,101 > G − counted + partial 60,100, a surplus of 1"
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
        if name in scenario.NO_WINDOW_ACTIONS:
            continue
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
                   on="standin", series_errors=None, count=None):
    """A clean apply and revert of one step on the stand-in, offsets from `T0`."""
    extra = {k: v for k, v in (("status", status), ("class", cls), ("answer", answer),
                               ("series_errors", series_errors), ("count", count))
             if v is not None}
    return [_action("apply", step, action, on, start, affects_egress=True, **extra),
            _action("revert", step, action, on, end - 1, end, affects_egress=True, **extra)]


def _standin_count(root):
    """A `respond` step's `count`: its rules in the scenario and on the control port, a counted
    cue spent by writes to the stand-in's path alone and then answering the default with no cue,
    the step's cue and timeline carrying it, and `ledger.standin` FAILing a counted step whose
    cue answered fewer writes than its count."""
    import http.client
    import http.server
    import io
    import threading
    server = _load_standin_server(root)
    for cue in ({"step": "x", "status": 400, "count": 0},
                {"step": "x", "status": 400, "count": True},
                {"step": "x", "status": 400, "count": "1"}):
        problems = server.cue_problems(cue)
        expect(any("`count` must be a positive integer" in p for p in problems),
               f"cue {cue} refused for its count, got {problems}")
    expect(server.cue_problems({"step": "x", "answer": "reset", "count": 2}) == [],
           "a counted reset is accepted")

    state = server.State("remote_write", "/api/v1/write")
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
            conn.request("POST", path, body=b"payload")
            return conn.getresponse().status
        finally:
            conn.close()

    try:
        rc, _ = faults.post_cue(control_base, {"step": "c0s1", "status": 400, "count": 2})
        answers = [post("/api/v1/write"), post("/api/v2/write"), post("/api/v1/write"),
                   post("/api/v1/write")]
        deadline = time.monotonic() + 5
        while (printed.getvalue().count('"event": "request"') < 4
               and time.monotonic() < deadline):
            time.sleep(0.01)
    finally:
        server.sys.stdout = real_stdout
        for one in (data, control):
            one.shutdown()
            one.server_close()
    requests = [json.loads(line) for line in printed.getvalue().splitlines()
                if '"event": "request"' in line]
    got = [(r["status"], r["cue"]) for r in requests]
    expect(rc == 0 and answers == [400, 404, 400, 200]
           and got == [(400, "c0s1"), (404, "c0s1"), (400, "c0s1"), (200, None)],
           f"a cue with count 2 answers two writes, a request off the path spends none, and "
           f"the next write gets the default with no cue, got {answers} {got}")

    path, base = _standin_base(root)
    counted = copy.deepcopy(base)
    counted["step"][0]["count"] = 1
    first = scenario.expand(scenario.from_dict(counted, path))[0]
    expect(first.cue() == {"step": "c0s1", "status": 503, "count": 1}
           and first.expected() == {"status": 503, "count": 1, "class": "ambiguous"},
           f"`count` travels in the cue and the timeline, got {first.cue()} "
           f"{first.expected()}")
    for value in (0, True, "1"):
        bad = copy.deepcopy(base)
        bad["step"][0]["count"] = value
        _refused(path, bad, "`count` must be a positive integer", f"a count of {value!r}")
    stop = copy.deepcopy(base)
    stop["step"][18]["count"] = 1
    _refused(path, stop, "`count` is for respond only", "a count on a stop")

    with tempfile.TemporaryDirectory() as tmp:
        def counted_run(name, cued):
            def edit(requests):
                for request in requests[:cued]:
                    request["cue"] = "c0s3"
                return requests
            # The fixture's four writes all fall inside the step's span.
            run_dir = _ledger_run(tmp, name, standin_edit=edit,
                                  timeline_extra=_respond_fault("c0s3", -10, 310, status=200,
                                                                count=2))
            return {r.id: r for r in checks.run_all(run_dir)}["ledger.standin"]

        row = counted_run("count-spent", 2)
        expect(row.status == checks.PASS, f"a counted cue that answered its count PASSes, got "
               f"{row}")
        row = counted_run("count-short", 1)
        expect(row.status == checks.FAIL
               and "its cue answered 1 write(s), not its count 2" in row.detail,
               f"a counted cue that answered fewer writes FAILs, got {row}")



def _standin_idle_timeout(root):
    """`data_handler`'s `idle_timeout`: the stand-in closes a keep-alive connection that stays
    idle past it, which `--idle-timeout` sets."""
    import http.server
    import io
    import socket
    import threading
    server = _load_standin_server(root)
    state = server.State("remote_write", "/api/v1/write")
    data = http.server.ThreadingHTTPServer(("127.0.0.1", 0), server.data_handler(state, 0.5))
    threading.Thread(target=data.serve_forever, daemon=True).start()
    real_stdout = server.sys.stdout
    server.sys.stdout = io.StringIO()
    closed_after = None
    response = b""
    try:
        with socket.create_connection(("127.0.0.1", data.server_address[1]), timeout=5) as sock:
            sock.sendall(b"POST /api/v1/write HTTP/1.1\r\nHost: standin\r\n"
                         b"Connection: keep-alive\r\nContent-Length: 7\r\n\r\npayload")
            while b"\r\n\r\n" not in response:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                response += chunk
            head, _, body = response.partition(b"\r\n\r\n")
            length = 0
            for line in head.split(b"\r\n")[1:]:
                name, _, value = line.partition(b":")
                if name.strip().lower() == b"content-length":
                    length = int(value)
            while len(body) < length:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                body += chunk
            sock.settimeout(3)
            started = time.monotonic()
            try:
                if sock.recv(4096) == b"":
                    closed_after = time.monotonic() - started
            except socket.timeout:
                pass
    finally:
        server.sys.stdout = real_stdout
        data.shutdown()
        data.server_close()
    expect(response.startswith(b"HTTP/1.1 200"),
           f"the keep-alive write is answered, got {response[:40]!r}")
    expect(closed_after is not None and closed_after < 2,
           f"an idle timeout of 0.5 s closes the idle connection well inside 3 s, closed after "
           f"{closed_after}")

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
               f"network_error at or above the resets PASSes, got {rows['ledger.standin']}")

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


def _written_counts(root):
    """Remote-write 2.0's written counts: the stand-in's default `-Written` headers on a 2.0
    write (none on a 1.0 write, a `202`, or a non-2xx), a cue's `headers` replacing them and its
    rules, the 2.0 `Request` walker and `unwritten_samples` on the host, `headers` in a
    scenario step, and `ledger.standin` holding a short count to the sink's
    `samples.dropped{reason="not_written"}`."""
    import http.client
    import http.server
    import io
    import threading
    server = _load_standin_server(root)
    v2_body = (root.parent.parent / "testdata" / "interop" / "prometheus"
               / "prometheus-v2-000.bin").read_bytes()
    v2_type = "application/x-protobuf;proto=io.prometheus.write.v2.Request"
    v1_type = "application/x-protobuf;proto=prometheus.WriteRequest"
    for cue, needle in (({"step": "x", "answer": "reset", "headers": {}}, "`status` only"),
                        ({"step": "x", "status": 200, "headers": []}, "maps header names"),
                        ({"step": "x", "status": 200, "headers": {"a": 1}}, "maps header names")):
        problems = server.cue_problems(cue)
        expect(any(needle in p for p in problems), f"cue {cue} refused with {needle!r}, got "
               f"{problems}")
    expect(server.cue_problems({"step": "x", "status": 200, "headers": {}}) == [],
           "a 2xx cue with empty headers is accepted")

    state = server.State("remote_write", "/api/v1/write")
    data = http.server.ThreadingHTTPServer(("127.0.0.1", 0), server.data_handler(state))
    control = http.server.ThreadingHTTPServer(("127.0.0.1", 0), server.control_handler(state))
    for one in (data, control):
        threading.Thread(target=one.serve_forever, daemon=True).start()
    printed = io.StringIO()
    real_stdout = server.sys.stdout
    server.sys.stdout = printed
    control_base = f"http://127.0.0.1:{control.server_address[1]}"
    written = "X-Prometheus-Remote-Write-Samples-Written"

    def post(content_type):
        conn = http.client.HTTPConnection("127.0.0.1", data.server_address[1], timeout=5)
        try:
            conn.request("POST", "/api/v1/write", body=v2_body,
                         headers={"Content-Encoding": "snappy", "Content-Type": content_type})
            response = conn.getresponse()
            response.read()
            return response.status, response.getheader(written)
        finally:
            conn.close()

    try:
        answers = [post(v2_type), post(v1_type)]
        for cue in ({"step": "c0s1", "status": 204, "headers": {}},
                    {"step": "c0s2", "status": 200, "headers": {written: "0"}},
                    {"step": "c0s3", "status": 202}, {"step": "c0s4", "status": 415}):
            faults.post_cue(control_base, cue)
            answers.append(post(v2_type))
        faults.post_cue(control_base, {})
        deadline = time.monotonic() + 5
        while (printed.getvalue().count('"event": "request"') < 6
               and time.monotonic() < deadline):
            time.sleep(0.01)
    finally:
        server.sys.stdout = real_stdout
        for one in (data, control):
            one.shutdown()
            one.server_close()
    expect(answers == [(200, "26"), (200, None), (204, None), (200, "0"), (202, None),
                       (415, None)],
           f"a 2.0 write gets its own count, a 1.0 write, a 202, and a 415 none, and a cue's "
           f"headers replace it, got {answers}")
    requests = [json.loads(line) for line in printed.getvalue().splitlines()
                if '"event": "request"' in line]
    recorded = [r.get("headers", {}).get(written) for r in requests]
    expect(recorded == ["26", None, None, "0", None, None],
           f"each record line carries the headers it answered with, got {recorded}")

    decoded = standin.decode_body(requests[0])
    expect(len(decoded) == 26 and decoded[0][0].get("__name__") == "go_gc_duration_seconds"
           and all(len(samples) == 1 for _, samples in decoded),
           f"the host decodes a 2.0 Request by its Content-Type, got {decoded[:1]}")
    expect([standin.unwritten_samples(r, decoded) for r in requests] == [0, 0, 0, 26, 0, 0],
           "only the 2xx whose Samples-Written is short names unwritten samples")

    loaded = scenario.load(root / "scenarios" / "standin-remote-write-v2" / "scenario.toml")
    first = scenario.expand(loaded)[0]
    expect(first.cue() == {"step": "c0s1", "status": 204, "apply": False, "headers": {}}
           and first.expected().get("headers") == {},
           f"headers travels in the cue and the timeline, got {first.cue()}")
    spec = scenario.StepSpec(index=0, at=0.0, action="respond", on="standin", args="",
                             for_=1.0, answer="reset", headers={})
    expect(any("`headers`" in p for p in scenario._respond_problems(spec, "step 1")),
           "a respond with headers and no status is refused")

    with tempfile.TemporaryDirectory() as tmp:
        def short(requests):
            requests[0].update(stored=False, cue="c0s2",
                               headers={"X-Prometheus-Remote-Write-Samples-Written": "0"})
            return requests

        timeline = _respond_fault("c0s2", 0, 400, status=200)

        def rows_for(name, sut_extra=()):
            run_dir = _ledger_run(tmp, name, standin_edit=short, timeline_extra=timeline,
                                  sut_extra=sut_extra)
            requests = standin.read_record((run_dir / "logs" / "standin.stdout").read_text())[0]
            unwritten = standin.unwritten_samples(requests[0], standin.decode_body(requests[0]))
            return {r.id: r for r in checks.run_all(run_dir)}, unwritten, requests[0]["t"]

        rows, unwritten, t = rows_for("short-uncounted")
        expect(unwritten > 0 and rows["ledger.standin"].status == checks.FAIL
               and "not written" in rows["ledger.standin"].detail
               and "never stored" not in rows["ledger.standin"].detail,
               f"a short count the sink didn't count FAILs, and isn't an unstored 2xx, got "
               f"{rows['ledger.standin']}")
        counted = [_line(_stamp(t + 1), "logit.output.samples.dropped", "sum", unwritten,
                         component="sink", attrs={"reason": "not_written"})]
        rows, _, _ = rows_for("short-counted", counted)
        expect(rows["ledger.standin"].status == checks.PASS
               and rows["drops"].status == checks.PASS,
               f"a short count the sink counted not_written PASSes ledger.standin and drops, "
               f"got {rows['ledger.standin']} {rows['drops']}")


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
        # datadog_out re-encodes a resend unchanged, so a new value at a stored second reads as
        # a second point of that second, the known gap, and is still summed.
        expect(rows["ledger.standin"].status == checks.FAIL
               and "Datadog keeps the later" in " ".join(rows["ledger.standin"].lines)
               and "a resend changed a sample" not in rows["ledger.standin"].detail
               and rows["ledger.egress"].status == checks.FAIL
               and "surplus" in rows["ledger.egress"].detail,
               f"a changed value at a stored second is summed, no last write winning: a "
               f"surplus, got "
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


def _influx_codec(root):
    """The `influxdb` shape's decoders: a body `influxdb_out` sent, from
    `standin/influxdb-vectors.json`, through `line_protocol` and `decode_body`; the hand-built
    escape and grammar cases and the bodies refused; precision, encoding, UTF-8, and label
    clashes; string and boolean fields; and the destination read from a query."""
    vectors = json.loads((root / "standin" / "influxdb-vectors.json").read_text())
    body = vectors["influxdb_out_body"]
    got = [[m, t, f, ts] for m, t, f, ts in standin.line_protocol(body)]
    expect(got == vectors["influxdb_out_body_expect"] and len(got) == 3,
           f"a body influxdb_out sent reads as its 3 points with the escaped tag, got {got}")
    request = {"body_b64": base64.b64encode(body.encode()).decode(), "content_encoding": None,
               "query": vectors["influxdb_out_query"]}
    series = standin.decode_body(request, standin.INFLUXDB)
    first = vectors["influxdb_out_body_expect"][0]
    expect(series[0] == ({"__name__": first[0], "field": "value", **first[1]},
                         [(first[3] // 1_000_000, first[2]["value"])]) and len(series) == 3,
           f"decode_body names a sample by measurement, field, and tags, in ms, got "
           f"{series[:1]}")
    expect(standin.influxdb_destination(request)
           == ("soak-org", "0123456789abcdef", "soak-carried")
           and standin.influxdb_destination({}) == (None, None, None),
           f"the destination is the query's org, orgID, and bucket, got "
           f"{standin.influxdb_destination(request)}")
    for case in vectors["cases"]:
        got = [[m, t, f, ts] for m, t, f, ts in standin.line_protocol(case["body"])]
        expect(got == case["points"], f"{case['about']}: got {got}")
    for case in vectors["refused"]:
        try:
            standin.line_protocol(case["body"])
            expect(False, f"line protocol {case['body']!r} was read")
        except standin.DecodeError as err:
            expect(case["error"] in str(err),
                   f"{case['body']!r} is refused with {case['error']!r}, got {err}")

    def decode(text, query="precision=ns", encoding=None):
        return standin.decode_body({"body_b64": base64.b64encode(text).decode(),
                                    "content_encoding": encoding, "query": query},
                                   standin.INFLUXDB)

    expect(decode(b"m value=1 7\n", "precision=s")[0][1] == [(7000, 1.0)]
           and decode(b"m value=1 7000000\n", "")[0][1] == [(7, 1.0)],
           "a timestamp is read at the query's precision, ns when absent")
    mixed = decode(b'm s="x",b=true,n=2i 1000000\n')
    expect([(labels["field"], samples) for labels, samples in mixed]
           == [("b", [(1, 1.0)]), ("n", [(1, 2.0)])],
           f"a string field isn't in V and a boolean reads as 1, got {mixed}")
    for text, query, encoding, needle in (
            (b"m value=1 1\n", "precision=h", None, "precision 'h'"),
            (b"m value=1 1\n", "", "gzip", "sends no compression"),
            (b"m,field=x value=1 1\n", "", None, "a label the harness names"),
            (b"m value=1 1\n\xff", "", None, "not UTF-8")):
        try:
            decode(text, query, encoding)
            expect(False, f"{text!r} under {query!r} {encoding} decoded")
        except standin.DecodeError as err:
            expect(needle in str(err), f"{text!r} is refused with {needle!r}, got {err}")


def _influx_points():
    """`stored_points` over an `influxdb` record: one term per series and timestamp, a
    same-value resend a rewrite naming both requests, a changed value a conflict whose last
    write is stored, an unstored or undecodable request left out of V; and
    `acknowledged_unstored` naming a 2xx point V lacks."""
    requests = [
        _influx_request(1, 10, [("a", 1.0, 1000), ("b", 5.0, 1000)]),
        _influx_request(2, 11, [("a", 1.0, 1000)], status=503, cue="c0s1"),
        _influx_request(3, 12, [("b", 6.0, 1000)]),
        _influx_request(4, 13, [("a", 3.0, 2000)], status=400, stored=False),
        {**_influx_request(5, 14, [("a", 4.0, 4000)]), "body_b64": "!!"},
        _influx_request(6, 15, [("a", 9.0, 5000)]),
    ]
    counts = standin.stored_points(requests)
    a = standin.series_key({"__name__": "a", "field": "value", "env": "soak"})
    b = standin.series_key({"__name__": "b", "field": "value", "env": "soak"})
    expect(sorted(counts.points) == [(1.0, 1.0), (1.0, 6.0), (5.0, 9.0)],
           f"V's terms are one per series and timestamp, the last write's, got {counts.points}")
    expect(counts.rewrites == [(a, 1000, 1.0, 0, 1)] and counts.duplicates == []
           and counts.conflicts == [(b, 1000, 5.0, 6.0)],
           f"a same-value resend is a rewrite and a changed one a conflict, got "
           f"{counts.rewrites} {counts.conflicts}")
    expect([seq for seq, _ in counts.undecodable] == [5],
           f"a stored body that doesn't decode is listed, got {counts.undecodable}")
    lost = standin.acknowledged_unstored(requests, counts.series, standin.INFLUXDB)
    expect(lost == [], f"every 2xx point is stored, got {lost}")
    without_a = [one for one in counts.series if one["metric"]["__name__"] != "a"]
    lost = standin.acknowledged_unstored(requests, without_a, standin.INFLUXDB)
    expect(sorted(stamp for _, stamp in lost) == [1000, 5000],
           f"a 2xx point missing from V is named, got {lost}")


def _influx_server(root):
    """The stand-in's `influxdb` shape on loopback ports: the default `204` with no
    `Content-Length` and no body on `/api/v2/write`, a second request on the same connection,
    the query split off the path and recorded, a cued status, and `404` off the path."""
    import http.client
    import http.server
    import io
    import threading
    server = _load_standin_server(root)
    state = server.State("influxdb", "/api/v2/write")
    data = http.server.ThreadingHTTPServer(("127.0.0.1", 0), server.data_handler(state))
    control = http.server.ThreadingHTTPServer(("127.0.0.1", 0), server.control_handler(state))
    for one in (data, control):
        threading.Thread(target=one.serve_forever, daemon=True).start()
    printed = io.StringIO()
    real_stdout = server.sys.stdout
    server.sys.stdout = printed
    control_base = f"http://127.0.0.1:{control.server_address[1]}"
    answers = []
    try:
        conn = http.client.HTTPConnection("127.0.0.1", data.server_address[1], timeout=5)
        try:
            for path in ("/api/v2/write?org=o&bucket=b&precision=ns",
                         "/api/v2/write?bucket=b"):
                conn.request("POST", path, body=b"m value=1 1\n")
                response = conn.getresponse()
                answers.append((response.status, response.getheader("Content-Length"),
                                response.read()))
            faults.post_cue(control_base, {"step": "c0s1", "status": 404,
                                           "body": '{"code":"not found"}'})
            conn.request("POST", "/api/v2/write?bucket=b", body=b"m value=1 1\n")
            response = conn.getresponse()
            answers.append((response.status, response.getheader("Content-Length"),
                            response.read()))
            faults.post_cue(control_base, {})
            conn.request("POST", "/api/v1/write?bucket=b", body=b"x")
            response = conn.getresponse()
            answers.append((response.status, None, response.read()[:8]))
        finally:
            conn.close()
        deadline = time.monotonic() + 5
        while (printed.getvalue().count('"event": "request"') < 4
               and time.monotonic() < deadline):
            time.sleep(0.01)
    finally:
        server.sys.stdout = real_stdout
        for one in (data, control):
            one.shutdown()
            one.server_close()
    expect(answers[:2] == [(204, None, b""), (204, None, b"")]
           and answers[2] == (404, "20", b'{"code":"not found"}') and answers[3][0] == 404,
           f"the influxdb shape answers 204 with no length twice on one connection, then the "
           f"cued 404 with its body, and 404 off its path, got {answers}")
    requests = [json.loads(line) for line in printed.getvalue().splitlines()
                if '"event": "request"' in line]
    got = [(r["path"], r["query"], r["status"], r["stored"]) for r in requests]
    expect(got == [("/api/v2/write", "org=o&bucket=b&precision=ns", 204, True),
                   ("/api/v2/write", "bucket=b", 204, True),
                   ("/api/v2/write", "bucket=b", 404, False),
                   ("/api/v1/write", "bucket=b", 404, False)],
           f"each record line holds the path without its query, and the query, got {got}")


def _influx_rules(root):
    """`[target] shape = "influxdb"` and its `class` rules: required on a respond other than a
    2xx, refused on a 2xx, and `series_errors` refused; each shipped `standin-influxdb*`
    scenario loading with its shape and every step classed but the 204."""
    for name in ("standin-influxdb", "standin-influxdb-carried", "standin-influxdb-mixed"):
        loaded = scenario.load(root / "scenarios" / name / "scenario.toml")
        unclassed = [s.status for s in loaded.steps if s.fault_class is None]
        expect(loaded.standin_shape == "influxdb" and unclassed == [204]
               and len(loaded.steps) == 19,
               f"{name} loads as an influxdb stand-in with 19 steps, all classed but the 204, "
               f"got {loaded.target} {unclassed}")
    path = root / "scenarios" / "standin-influxdb" / "scenario.toml"
    base = tomllib.loads(path.read_text())

    def step(index, **changes):
        raw = copy.deepcopy(base)
        for key in [k for k, v in changes.items() if v is None]:
            raw["step"][index].pop(key, None)
        raw["step"][index].update({k: v for k, v in changes.items() if v is not None})
        return raw

    for raw, needle, what in (
            (step(1, **{"class": None}),
             "under the influxdb shape a respond other than a 2xx needs `class`", "a 400"),
            (step(17, **{"class": None}),
             "under the influxdb shape a respond other than a 2xx needs `class`", "a reset"),
            (step(0, **{"class": "ambiguous"}), "a 2xx isn't a fault, so it takes no `class`",
             "a class on a 204"),
            (step(0, series_errors=1), "`series_errors` is for a standin target with shape "
             "'datadog'", "series_errors")):
        _refused(path, raw, needle, what)


def _influx_checks():
    """The checks over an `influxdb` stand-in two-life run: V as one stored point per series
    and timestamp, `ledger.sent` SKIPping, an applied 503's resend reported as a rewrite, a
    rewrite after a 2xx or under `at_most_once` FAILing, a changed resend FAILing, a rejected
    step's points absent from the store and equal to the gap, an unexplained gap and a stored
    rejected point FAILing, and two destinations FAILing."""
    with tempfile.TemporaryDirectory() as tmp:
        def drop(offset, n=1):
            return [_line(_stamp(T0 + offset), "logit.component.batches.dropped", "sum", n,
                          component="sink", attrs={"reason": "rejected"}),
                    _line(_stamp(T0 + offset), "logit.component.batches.received", "sum", n,
                          component="sink")]

        def results(name, edit=lambda requests: requests, sut_config=None, **kwargs):
            run_dir = _ledger_run(tmp, name, standin_edit=edit, standin_shape="influxdb",
                                  **kwargs)
            if sut_config is not None:
                (run_dir / "configs").mkdir(exist_ok=True)
                (run_dir / "configs" / "sut.yaml").write_text(sut_config)
                resolved = json.loads((run_dir / "scenario.resolved.json").read_text())
                resolved["configs"] = {"sut": "sut.yaml"}
                (run_dir / "scenario.resolved.json").write_text(json.dumps(resolved))
            return {r.id: r for r in checks.run_all(run_dir)}

        rows = results("influx-clean")
        for row in ("ledger.egress", "ledger.summary", "ledger.standin", "drops"):
            expect(rows[row].status == checks.PASS,
                   f"an influxdb stand-in run that balances PASSes {row}, got {rows[row]}")
        expect(rows["ledger.sent"].status == checks.SKIP
               and "shape influxdb" in rows["ledger.sent"].detail,
               f"ledger.sent SKIPs under the influxdb shape, got {rows['ledger.sent']}")
        expect("0 point(s) rewritten" in rows["ledger.standin"].detail
               and "destination org='soak-org', bucket='soak'" in rows["ledger.standin"].detail
               and "one per series and timestamp" in " ".join(rows["ledger.egress"].lines),
               f"the row reports rewrites and the one destination, got "
               f"{rows['ledger.standin'].detail}")

        # A 503 after storing (`apply`), resent: InfluxDB overwrites, so V counts it once.
        def resent(requests):
            again = copy.deepcopy(requests[-1])
            requests[-1].update(status=503, cue="c0s2")
            again.update(seq=99, t=again["t"] + 0.5)
            return requests + [again]

        applied = _respond_fault("c0s2", 290, 305, status=503)
        rows = results("influx-rewrite", resent, timeline_extra=applied)
        expect(rows["ledger.standin"].status == checks.PASS
               and rows["ledger.egress"].status == checks.PASS
               and "2 point(s) rewritten with the same value (life 1 2)"
               in rows["ledger.standin"].detail,
               f"a resend after an applied 503 is a reported rewrite, not a duplicate, got "
               f"{rows['ledger.standin']} {rows['ledger.egress']}")
        amo = ("components:\n  window: { type: aggregate, interval: 10s }\n"
               "  sink:\n    type: influxdb_out\n    buffer: { delivery: at_most_once }\n")
        rows = results("influx-rewrite-amo", resent, timeline_extra=applied, sut_config=amo)
        expect(rows["ledger.standin"].status == checks.FAIL
               and "a resend under at_most_once" in rows["ledger.standin"].detail,
               f"a rewrite under at_most_once FAILs, got {rows['ledger.standin']}")

        def resent_accepted(requests):
            again = copy.deepcopy(requests[-1])
            again.update(seq=99, t=again["t"] + 0.5)
            return requests + [again]

        rows = results("influx-rewrite-2xx", resent_accepted)
        expect(rows["ledger.standin"].status == checks.FAIL
               and "the sink resent a point it saw accepted" in rows["ledger.standin"].detail,
               f"a rewrite of a point the stand-in answered 2xx FAILs, got "
               f"{rows['ledger.standin']}")

        def changed(requests):
            again = copy.deepcopy(requests[-1])
            text = base64.b64decode(again["body_b64"]).decode().replace(" value=", " value=7")
            again.update(seq=99, t=again["t"] + 0.5,
                         body_b64=base64.b64encode(text.encode()).decode())
            return requests + [again]

        rows = results("influx-changed", changed)
        expect(rows["ledger.standin"].status == checks.FAIL
               and "a resend changed a sample" in rows["ledger.standin"].detail,
               f"a resend storing another value FAILs, got {rows['ledger.standin']}")

        # A 400 rejecting the last request inside a step classed rejected: its points are the
        # final life's whole gap.
        def rejected(requests):
            requests[-1].update(status=400, stored=False, cue="c0s2")
            return requests

        cued = _respond_fault("c0s2", 290, 305, status=400, cls="rejected")
        rows = results("influx-rejected", rejected, timeline_extra=cued, sut_extra=drop(300))
        for row in ("ledger.standin", "ledger.egress", "ledger.summary", "drops"):
            expect(rows[row].status == checks.PASS,
                   f"a cued rejection the gap equals PASSes {row}, got {rows[row]}")
        lines = " ".join(rows["ledger.standin"].lines)
        expect(re.search(r"life 1: egress gap ([0-9,]+), the rejected requests' points sum "
                         r"\1(?![0-9,])", lines) is not None,
               f"the row reports the final life's gap equal to the rejected points, got {lines}")

        def rejected_more(requests):
            requests = rejected(requests)
            text = base64.b64decode(requests[-1]["body_b64"]).decode()
            text += f"x_9,env=soak value=5.0 {int((T0 + 299) * 1000) * 1_000_000}\n"
            requests[-1]["body_b64"] = base64.b64encode(text.encode()).decode()
            return requests

        rows = results("influx-rejected-gap", rejected_more, timeline_extra=cued,
                       sut_extra=drop(300))
        expect(rows["ledger.standin"].status == checks.FAIL
               and "a gap the rejected steps don't explain" in rows["ledger.standin"].detail
               and rows["ledger.egress"].status == checks.PASS,
               f"a gap other than the rejected points FAILs ledger.standin, where "
               f"ledger.egress counts it, got {rows['ledger.standin']} {rows['ledger.egress']}")

        def rejected_stored(requests):
            again = copy.deepcopy(requests[-1])
            requests = rejected(requests)
            again.update(seq=99, t=T0 + 300.5)
            return requests + [again]

        rows = results("influx-rejected-stored", rejected_stored, timeline_extra=cued,
                       sut_extra=drop(300))
        expect(rows["ledger.standin"].status == checks.FAIL
               and "the sink resent a rejected batch" in rows["ledger.standin"].detail,
               f"a rejected request's point in the store FAILs, got {rows['ledger.standin']}")

        def two_buckets(requests):
            requests[0]["query"] = "org=soak-org&bucket=other&precision=ns"
            return requests

        rows = results("influx-two-buckets", two_buckets)
        expect(rows["ledger.standin"].status == checks.FAIL
               and "the writes name 2 destinations, one expected" in rows["ledger.standin"].detail
               and "bucket='other' (1 write(s))" in rows["ledger.standin"].detail,
               f"writes to two buckets FAIL, got {rows['ledger.standin']}")


# A rolling overlap: two instances, `logit` and `logit-b`, in `sut-net`'s namespace.
OVERLAP = {"holder": "sut-net", "instances": ["logit", "logit-b"], "label": "soak_instance"}
OVERLAP_ROLES = {**scenario.BASE_ROLES, "logit-b": "sut", "sut-net": "netns"}
OVERLAP_MODES = {"logit": "service:sut-net", "logit-b": "service:sut-net"}


def _overlap_dir(root, tmp, name="overlap", sut_edit=None, own_b=None):
    """A copy of rolling-overlap in `tmp/name`, its SUT config passed through `sut_edit`, and
    with `own_b`, a config of logit-b's own, `logit-sut-b.yaml`, named in `[configs]`."""
    base = root / "scenarios" / "rolling-overlap"
    directory = Path(tmp) / name
    directory.mkdir()
    for path in base.iterdir():
        (directory / path.name).write_text(path.read_text())
    sut = (base / "logit-sut.yaml").read_text()
    if sut_edit is not None:
        (directory / "logit-sut.yaml").write_text(sut_edit(sut))
    if own_b is not None:
        (directory / "logit-sut-b.yaml").write_text(own_b(sut))
        text = (directory / "scenario.toml").read_text()
        marker = 'generator = "logit-generator.yaml"\n'
        (directory / "scenario.toml").write_text(
            text.replace(marker, marker + 'logit-b = "logit-sut-b.yaml"\n', 1))
    return directory / "scenario.toml"


def _overlap_rules(root):
    """`[overlap]`'s rules: the table's own, against the ledger and each instance's config, the
    steps on the holder and on the instances, the role map and each instance's network_mode,
    list-valued generator keys, and the fault predicates, under which every instance is an
    ingress SUT and the holder's netem and partition reach every instance."""
    path = root / "scenarios" / "rolling-overlap" / "scenario.toml"
    raw = tomllib.loads(path.read_text())
    loaded = scenario.load(path)
    expect(loaded.suts == ["logit", "logit-b"] and loaded.extra_suts == []
           and loaded.sut_config_path("logit-b").name == "logit-sut.yaml"
           and loaded.to_json()["overlap"] == OVERLAP
           and loaded.to_json()["suts"] == ["logit", "logit-b"],
           f"the overlap's instances are its SUT services, sharing sut's config, got "
           f"{loaded.suts} {loaded.extra_suts}")
    expect(scenario.service_problems(loaded, OVERLAP_ROLES, OVERLAP_MODES) == [],
           f"the overlap's stack holds, got "
           f"{scenario.service_problems(loaded, OVERLAP_ROLES, OVERLAP_MODES)}")

    def mutate(edit):
        changed = copy.deepcopy(raw)
        edit(changed)
        return changed

    def step(**fields):
        return lambda r: r["step"].append({"for": "20s", **fields})

    def refused(edit, needle, what, roles=OVERLAP_ROLES):
        _refused(path, mutate(edit), needle, what, roles=roles)

    refused(lambda r: r["overlap"].update(extra=1), "overlap: unknown key `extra`",
            "an unknown overlap key")
    refused(lambda r: r["overlap"].pop("label"), "overlap: missing `label`", "no label")
    refused(lambda r: r["overlap"].update(label="soak-instance"),
            "overlap.label must be a Prometheus label name", "a label that isn't a label name")
    refused(lambda r: r["overlap"].update(instances=["logit"]),
            "overlap.instances must list two or more distinct", "one instance")
    refused(lambda r: r["overlap"].update(instances=["logit", "logit"]),
            "overlap.instances must list two or more distinct", "a repeated instance")
    refused(lambda r: r["overlap"].update(instances=["logit-b", "logit"]),
            "overlap.instances must start with logit", "logit not first")
    refused(lambda r: r["overlap"].update(holder="logit-b"),
            "overlap.holder can't be an instance", "the holder an instance")
    refused(lambda r: r["ledger"].update(hop=[dict(TWO_SUT_HOP)]),
            "[overlap] and [[ledger.hop]] can't be combined", "an overlap beside a hop")
    refused(lambda r: r["configs"].update(collector="logit-sut.yaml"),
            "configs.collector: under [overlap]", "a further SUT that isn't an instance")
    refused(lambda r: r["ledger"]["source"][0].update(wire="tcp"),
            "under [overlap] a source's wire is udp", "an overlap on a TCP wire")
    refused(lambda r: r["ledger"]["source"][0].update(sut_transforms=["other"]),
            "`sut_transforms` must list the set component stamping 'soak_instance' (instance)",
            "the label's set off the source's transforms")
    refused(lambda r: r["ledger"]["source"][0].update(generator_input=[]),
            "`generator_input` must name a component id or list distinct ones",
            "an empty generator_input list")
    refused(lambda r: r["ledger"]["source"][0].update(generator_sink=["statsd_0", "statsd_0"]),
            "`generator_sink` must name a component id or list distinct ones",
            "a repeated generator_sink")
    refused(lambda r: r["ledger"]["source"][0].update(generator_sink=["statsd_0", "statsd_1"]),
            "`generator_input` and `generator_sink` list the same number of components",
            "eight inputs paired with two sinks")
    refused(lambda r: r["ledger"]["source"][0].update(generator_input="load_0"),
            "`generator_input` and `generator_sink` list the same number of components",
            "one input beside a list of sinks")
    refused(step(at="4m", action="netem", on="logit", args="delay 10ms"),
            "an instance shares sut-net's network namespace, so put netem on sut-net",
            "netem on an instance")
    refused(step(at="4m", action="partition", on="logit-b"),
            "so put partition on sut-net", "a partition of an instance")
    refused(step(at="4m", action="stop", on="sut-net"),
            "the holder takes netem and partition only", "a stop of the holder")
    refused(step(at="20s", action="pause", on="logit-b"),
            "stop on logit and pause on logit-b overlap; with every instance held",
            "lifecycle faults on two instances overlapping")
    accepted = mutate(lambda r: (r["step"].append({"at": "4m", "action": "partition",
                                                   "on": "sut-net", "for": "20s"}),
                                 r["step"].append({"at": "4m30s", "action": "kill",
                                                   "on": "logit", "for": "20s"})))
    try:
        loaded_kill = scenario.from_dict(accepted, path)
        expect(scenario.service_problems(loaded_kill, OVERLAP_ROLES, OVERLAP_MODES) == [],
               "a partition of the holder and a kill of an instance are allowed")
    except scenario.ScenarioError as err:
        expect(False, f"a holder partition and an instance kill are refused: {err.problems}")
    refused(lambda r: None, "overlap.holder sut-net must be the stack's one netns service, and "
            "the stack's are none", "a holder the stack doesn't run as netns",
            roles={**OVERLAP_ROLES, "sut-net": "backend"})
    problems = scenario.service_problems(loaded, OVERLAP_ROLES,
                                         {**OVERLAP_MODES, "logit-b": "bridge"})
    expect(any("overlap instance logit-b needs network_mode \"service:sut-net\"" in p
               for p in problems),
           f"an instance outside the holder's namespace is refused, got {problems}")
    statsd_path, statsd_raw = _base(root)
    _refused(statsd_path, statsd_raw, "the netns role is an [overlap] holder's",
             "a netns service with no [overlap]", roles={**scenario.BASE_ROLES, "ns": "netns"})

    with tempfile.TemporaryDirectory() as tmp:
        def config_refused(name, needle, what, **kwargs):
            try:
                scenario.load(_overlap_dir(root, tmp, name, **kwargs))
                expect(False, f"{what}: accepted")
            except scenario.ScenarioError as err:
                expect(any(needle in p for p in err.problems),
                       f"{what}: expected {needle!r}, got {err.problems}")

        config_refused("no-reuse", "listener 'statsd' needs `reuse_port: true`",
                       "a listener without reuse_port",
                       sut_edit=lambda t: t.replace(", reuse_port: true", ""))
        config_refused("no-delay", "needs a `shutdown.delay` above 0s", "no shutdown delay",
                       sut_edit=lambda t: t.replace("shutdown: { delay: 5s }\n", ""))
        config_refused("long-delay", "`shutdown.delay` 25s plus the 10s drain must be under "
                       "docker stop's 30s", "a delay past docker stop's timeout",
                       sut_edit=lambda t: t.replace("delay: 5s", "delay: 25s"))
        config_refused("admin-bind", "admin needs `socket:` and no `bind:`",
                       "an admin port on the shared address",
                       sut_edit=lambda t: t.replace("{ socket: /tmp/admin.sock }",
                                                    "{ bind: 127.0.0.1:9600 }"))
        config_refused("no-set", "no `set` component stamps 'soak_instance'", "no label setter",
                       sut_edit=lambda t: t.replace("soak_instance:", "other_label:"))
        config_refused("b-differs", "overlap: logit-sut-b.yaml and logit-sut.yaml differ in "
                       "more than the lines setting 'soak_instance'",
                       "an instance config that differs past its label",
                       own_b=lambda t: t.replace("interval: 10s", "interval: 5s"))
        own = scenario.load(_overlap_dir(
            root, tmp, "b-own", own_b=lambda t: t.replace("!env SOAK_INSTANCE", "logit-b")))
        expect(own.extra_suts == ["logit-b"] and own.suts == ["logit", "logit-b"]
               and own.sut_config_path("logit-b").name == "logit-sut-b.yaml",
               f"an instance may run its own config differing in its label alone, got "
               f"{own.extra_suts}")

    for action, on, udp, tcp, egress in (
            ("stop", "logit-b", True, True, True), ("kill", "logit-b", True, True, True),
            ("pause", "logit", True, True, True), ("netem", "sut-net", False, True, True),
            ("partition", "sut-net", True, True, True)):
        one = scenario.Step("c0s1", 0, 0, action, on, "", 0, 1)
        got = (faults.ACTIONS[action].affects_udp_ingress(one, OVERLAP_ROLES),
               faults.ACTIONS[action].affects_tcp_ingress(one, OVERLAP_ROLES),
               faults.ACTIONS[action].affects_egress(one, OVERLAP_ROLES))
        expect(tuple(map(bool, got)) == (udp, tcp, egress),
               f"a {action} on {on} under an overlap affects UDP {udp}, TCP {tcp}, egress "
               f"{egress}, got {got}")
    one = scenario.Step("c0s1", 0, 0, "stop", "collector", "", 0, 1)
    expect(not faults.ACTIONS["stop"].affects_udp_ingress(one, TWO_SUT_ROLES),
           "with no holder a further SUT still affects no UDP ingress")


def _overlap_driver(root):
    """The driver under `[overlap]`: `compose.env` has no variable for an instance sharing
    sut's config, the resolved scenario records the overlap, compose's network modes are read
    beside the roles, readiness is probed with the container's own `LOGIT_ADMIN`, and the end
    sequence waits once on the backends' totals and then stops each instance."""
    loaded = scenario.load(root / "scenarios" / "rolling-overlap" / "scenario.toml")
    with tempfile.TemporaryDirectory() as tmp:
        run = driver.Run(root, loaded, loaded.duration, None, False, Path(tmp) / "out", [],
                         "logit:soak", roles=OVERLAP_ROLES)
        run.prepare()
        env = (run.run_dir / "compose.env").read_text()
        resolved = json.loads((run.run_dir / "scenario.resolved.json").read_text())
        expect("SOAK_LOGIT_B_CONFIG" not in env and resolved["overlap"] == OVERLAP
               and resolved["suts"] == ["logit", "logit-b"],
               f"compose.env and the resolved scenario under an overlap, got {env} "
               f"{resolved.get('overlap')}")
        for jsonl in (run.timeline, run.watchdog, run.stats, run.freshness, run.capture):
            jsonl.close()

        resolver = driver.Run(root, loaded, loaded.duration, None, False, "/nonexistent", [],
                              "logit:soak")
        text = json.dumps({"services": {name: {"labels": {"soak.role": role},
                                               **({"network_mode": OVERLAP_MODES[name]}
                                                  if name in OVERLAP_MODES else {})}
                                        for name, role in OVERLAP_ROLES.items()}})
        resolver.docker = _ConfigDocker(docker.Result(0, text, ""))
        expect(resolver.resolve_roles() == OVERLAP_ROLES and resolver.modes == OVERLAP_MODES,
               f"resolve_roles reads the network modes too, got {resolver.modes}")

        class Execs:
            def __init__(self):
                self.argv = []

            def exec_(self, container, argv, timeout=None):
                self.argv.append(argv)
                return docker.Result(0, "ok", "")

        probe = driver.Run(root, loaded, loaded.duration, None, False, "/nonexistent", [],
                           "logit:soak", roles=OVERLAP_ROLES)
        probe.docker = Execs()
        probe.ids = {"logit-b": "id-b"}
        probe.timeline = _Records()
        probe.t0 = 0.0
        probe.pending_ready = {"logit-b": {"since": driver._now(), "step": "c0s2",
                                           "why": "start"}}
        real_print = driver._print
        driver._print = lambda text, stream=None: None
        try:
            probe.poll_ready()
        finally:
            driver._print = real_print
        expect(probe.docker.argv == [["logit", "ready"]] and probe.timeline[-1]["ok"],
               f"readiness is probed with the container's LOGIT_ADMIN, got {probe.docker.argv}")

        events = []
        seq = driver.Run(root, loaded, loaded.duration, None, False, tempfile.mkdtemp(dir=tmp),
                         [], "logit:soak", roles=OVERLAP_ROLES)
        seq.run_dir = driver.claim_run_dir(seq.out_dir)
        seq.docker = _ChainDocker([1000.0], events, lambda now: [])
        seq.ids = {name: f"id-{name}" for name in OVERLAP_ROLES}
        seq.timeline = _Records()
        seq.t0 = 900.0
        seq.inspect_all = lambda: None
        seq.wait_quiet = lambda: events.append(("quiet",))
        seq.refresh_vm_base = lambda backend: None
        reals = (driver._print, vm.force_flush, vm.export_raw)
        driver._print = lambda text, stream=None: None
        vm.force_flush = lambda base: None
        vm.export_raw = lambda base, selector, start, timeout=None: ""
        try:
            seq.end_sequence()
        finally:
            driver._print, vm.force_flush, vm.export_raw = reals
        order = [e for e in events if e[0] in ("stop", "quiet")]
        stopped = [r.get("service") for r in seq.timeline if r.get("phase") == "sut_stopped"]
        expect(order == [("stop", "id-generator"), ("quiet",), ("stop", "id-logit"),
                         ("stop", "id-logit-b")] and stopped == ["logit", "logit-b"],
               f"the generators, one wait on the backends, then each instance, got {order}")


# The overlap fixture: logit stopped (or killed) over `STOP`; each instance drains every 5 s.
OVERLAP_SUT_CONFIG = ("admin: { socket: /tmp/admin.sock }\n"
                      "components:\n"
                      "  statsd: { type: statsd_in, reuse_port: true }\n"
                      "  window:\n"
                      "    type: aggregate\n"
                      "    interval: 10s\n")


def _overlap_run(tmp, name, loss=0, used=2048, action="stop", unlabeled=False, stall=False,
                 end_extra=0):
    """A two-instance run: the generator's two inputs and sinks send `PER_DRAIN` lines per 5 s
    drain from +5 to +300, `logit` reads 600 of them a drain and `logit-b` 400, until `logit`
    stops over `STOP` (or `action`, a kill), when `logit-b` reads all 1,000 a drain, `loss`
    fewer in its first drain after the stop, what the close discarded. `logit`'s last drain
    before the stop samples `used` bytes in its receive queue. Each instance stamps its series
    with its name; `unlabeled` adds a series with no label. Under `stall`, `logit-b` delivers
    nothing from +95 to +180 with a batch queued. `end_extra` lines reach `logit-b` at +310,
    after the generator stops, a surplus over G at the end."""
    gen = []
    for offset in range(5, 301, 5):
        stamp = _stamp(T0 + offset)
        gen.append(_line(stamp, "logit.process.uptime", "gauge", float(offset)))
        for index in (0, 1):
            for metric in ("logit.output.messages", "logit.output.datagrams"):
                gen.append(_line(stamp, metric, "sum", PER_DRAIN // 2, component=f"out_{index}"))
            gen.append(_line(stamp, "logit.component.events.sent", "sum", PER_DRAIN // 2,
                             component=f"load_{index}"))
            gen.append(_line(stamp, "logit.component.batches.sent", "sum", PER_DRAIN // 200,
                             component=f"load_{index}"))
            gen.append(_line(stamp, "logit.component.buffer.batches", "gauge", 0,
                             component=f"out_{index}"))

    def instance(rows):
        """NDJSON for one instance from `[(offset, life, life_start, w, delivered, queued,
        used)]`."""
        out = []
        for offset, life, start, w, delivered, queued, sampled in rows:
            stamp = _stamp(T0 + offset)
            out.append(_line(stamp, "logit.process.uptime", "gauge", float(offset - start)))
            for metric, value, comp in (("logit.input.datagrams", w, "statsd"),
                                        ("logit.component.events.sent", w, "statsd"),
                                        ("logit.component.events.received", w, "instance"),
                                        ("logit.component.events.sent", w, "instance"),
                                        ("logit.component.events.received", w, "window"),
                                        ("logit.transform.metrics.absorbed", w, "window"),
                                        ("logit.component.events.sent", 1, "window"),
                                        ("logit.component.events.received", 1, "sink"),
                                        ("logit.component.batches.received", 1, "sink"),
                                        ("logit.component.batches.delivered", delivered,
                                         "sink")):
                out.append(_line(stamp, metric, "sum", value, component=comp))
            for metric, value, comp in (("logit.component.buffer.batches", queued, "sink"),
                                        ("logit.component.buffer.utilization", 0, "sink"),
                                        ("logit.component.retrying", 0, "sink"),
                                        ("logit.component.receive.utilization", 0, "statsd"),
                                        ("logit.input.receive_buffer.used.bytes", sampled,
                                         "statsd"),
                                        ("logit.input.receive_buffer.bytes", 212992,
                                         "statsd")):
                out.append(_line(stamp, metric, "gauge", value, component=comp))
        return out

    a_rows, b_rows = [], []
    w_a = {0: 0, 1: 0}
    w_b = 0
    for offset in range(5, 321, 5):
        sending = offset <= 300
        down = STOP[0] < offset <= STOP[1] + 4
        if offset <= STOP[0]:
            w = 600 if sending else 0
            a_rows.append((offset, 0, -1, w, 1, 0, used if offset == 100 else 0))
            w_a[0] += w
        elif not down:
            w = 600 if sending else 0
            a_rows.append((offset, 1, STOP[1], w, 1, 0, 0))
            w_a[1] += w
        w = (400 if not down else 1000 - (loss if offset == 105 else 0)) if sending else 0
        if offset == 310:
            w += end_extra
        stalled = stall and 95 <= offset <= 180
        b_rows.append((offset, 0, -1, w, 0 if stalled else 1, 1 if stalled else 0, 0))
        w_b += w
    a_stdout = instance(a_rows)
    timeline = [_phase("start", 0, t0=T0),
                _action("apply", "c0s1", action, "logit", STOP[0], affects_udp_ingress=True,
                        affects_egress=True),
                _action("revert", "c0s1", action, "logit", STOP[1] - 1, STOP[1],
                        affects_udp_ingress=True, affects_egress=True),
                _ready("c0s1", "logit", STOP[1] + 1),
                _phase("end_begin", 300), _phase("quiet", 305, held=True),
                _phase("sut_stopped", 325, service="logit"),
                _phase("sut_stopped", 326, service="logit-b"), _phase("end_end", 330)]
    ledger = {**LEDGER, "source": [{"service": "generator",
                                    "generator_input": ["load_0", "load_1"],
                                    "generator_sink": ["out_0", "out_1"],
                                    "sut_listener": "statsd", "sut_transforms": ["instance"]}],
              "sink": [{"id": "sink", "backend": "victoria-metrics",
                        "vm_selector": "{__name__=~\"x_[0-9]+_total\"}"}]}
    run_dir = _run_dir(tmp, name, timeline, a_stdout, ledger=ledger, services=OVERLAP_ROLES,
                       suts=["logit", "logit-b"], overlap=OVERLAP,
                       configs={"sut": "logit-sut.yaml"})
    (run_dir / "configs").mkdir()
    (run_dir / "configs" / "logit-sut.yaml").write_text(OVERLAP_SUT_CONFIG)
    (run_dir / "logs" / "logit-b.stdout").write_text("\n".join(instance(b_rows)) + "\n")
    (run_dir / "logs" / "generator.stdout").write_text("\n".join(gen) + "\n")

    def series(instance_name, segments):
        values, stamps = [], []
        for total, offsets in segments:
            for i, s in enumerate(offsets):
                values.append(total * (i + 1) // len(offsets))
                stamps.append(int((T0 + s) * 1000))
        labels = {"__name__": "x_0_total"}
        if instance_name is not None:
            labels["soak_instance"] = instance_name
        return {"metric": labels, "values": values, "timestamps": stamps}

    export = [series("logit", [(w_a[0], (50, 100)), (w_a[1], (200, 300))]),
              series("logit-b", [(w_b, (50, 100, 200, 300))])]
    if unlabeled:
        export.append(series(None, [(10, (50, 100))]))
    _write_export(run_dir, "sink", export)
    return run_dir


def _overlap_checks():
    """The checks over a two-instance run: each per-life row on each instance's own lives and
    labeled series, `ledger.wire` and `recovery` on the instances' summed ingest, and
    `ledger.overlap` judging the run's gap against the close's bound: a clean swap passes, a
    loss within the bound passes and one past it FAILs, a kill is reported and not judged, a
    series with no instance's label FAILs `ledger.egress`, and a stall of the only instance
    running FAILs `progress`."""
    with tempfile.TemporaryDirectory() as tmp:
        rows = {r.id: r for r in checks.run_all(_overlap_run(tmp, "clean"))}
        for row in ("ledger.wire", "ledger.overlap", "ledger.intake", "ledger.edge",
                    "ledger.aggregate", "ledger.egress", "ledger.summary", "identity.sink",
                    "progress", "recovery", "drops"):
            expect(row in rows and rows[row].status == checks.PASS,
                   f"{row} PASSes a clean swap, got {rows.get(row)}")
        expect("ledger.hop" not in rows, "an overlap run has no ledger.hop row")
        detail = rows["ledger.overlap"].detail
        expect(detail.startswith("G 60,000 − Σ(W + K + F) 60,000 (logit 31,800, logit-b "
                                 "28,200) = 0, within [-")
               and ", 5]: 1 close(s)" in detail,
               f"ledger.overlap sums both instances against the generators' two sinks, got "
               f"{detail}")
        lines = "\n".join(rows["ledger.overlap"].lines)
        expect("c0s1 stop on logit at +101s, life 0: gap 0 in the drain intervals across it "
               "(reported); last logit.input.receive_buffer.used.bytes 2,048 -> at most 5 "
               "datagram(s) discarded (loose by up to 104" in lines,
               f"ledger.overlap reports each stop's own numbers, got {lines}")
        detail = rows["ledger.egress"].detail
        expect("logit: sink: life 0: W − D − B − X − ΣT_disc − V 0" in detail
               and "life 1 (final): Ab 19,800 − V[sink] 19,800 = 0, ok" in detail
               and "logit-b: sink: life 0 (final): Ab 28,200 − V[sink] 28,200 = 0, ok" in detail,
               f"ledger.egress judges each instance against its labeled series, got {detail}")
        expect("logit: final life 1: W − D 19,800 vs E + B 19,800" in rows["ledger.intake"].detail
               and "logit-b: final life 0: W − D 28,200" in rows["ledger.intake"].detail,
               f"ledger.intake judges each instance's own final life, got "
               f"{rows['ledger.intake'].detail}")
        expect("end to end over the run, G 60,000 less the instances' counted loss 0: sink: ΣV "
               "60,000 <= G − counted 60,000" in rows["ledger.summary"].detail
               and "wire G − Σ(W + K + F) 0 over the run" in rows["ledger.summary"].detail,
               f"ledger.summary bounds V summed over the instances, got "
               f"{rows['ledger.summary'].detail}")
        expect("W by instance: logit 31,800, logit-b 28,200" in rows["ledger.wire"].detail,
               f"ledger.wire reads both instances, got {rows['ledger.wire'].detail}")
        lines = "\n".join(rows["recovery"].lines)
        expect("c0s1 (stop on logit): logit sink recovered at" in lines
               and "c0s1 (stop on logit): logit-b sink recovered at" in lines
               and "ingest 200/s (100%)" in lines,
               f"recovery judges each instance on its own drains against the summed ingest, "
               f"got {lines}")
        expect("on the instances running through each" in rows["progress"].detail,
               f"progress judges the instances running, got {rows['progress'].detail}")
        led = checks.Ledger(checks.instance_view(checks.RunData(_overlap_run(tmp, "view")),
                                                 "logit-b"))
        expect(led.problems == [] and led.service == "logit-b" and led.lives == [0]
               and led.gen_stats["generator"]["G"] == 60000
               and led.gen_stats["generator"]["batch"] == 200,
               f"an instance's view reads its own lives and the generator's summed sinks, got "
               f"{led.problems} {led.lives} {led.gen_stats}")

        within = {r.id: r for r in checks.run_all(_overlap_run(tmp, "within", loss=5))}
        expect(within["ledger.overlap"].status == checks.PASS
               and "= 5, within [-" in within["ledger.overlap"].detail
               and within["ledger.wire"].status == checks.PASS,
               f"a loss at the close within its bound passes, got {within['ledger.overlap']}")
        over = {r.id: r for r in checks.run_all(_overlap_run(tmp, "over", loss=6))}
        expect(over["ledger.overlap"].status == checks.FAIL
               and "= 6, uncounted, over the closes' 5" in over["ledger.overlap"].detail,
               f"a loss past the close's bound FAILs, got {over['ledger.overlap']}")
        killed = {r.id: r for r in checks.run_all(_overlap_run(tmp, "killed", loss=50,
                                                               action="kill"))}
        expect(killed["ledger.overlap"].status == checks.SKIP
               and "reported, not judged: c0s1 kill on logit" in killed["ledger.overlap"].detail
               and "= 50" in killed["ledger.overlap"].detail,
               f"a kill of an instance is reported, not judged, got {killed['ledger.overlap']}")
        unlabeled = {r.id: r for r in checks.run_all(_overlap_run(tmp, "unlabeled",
                                                                  unlabeled=True))}
        expect(unlabeled["ledger.egress"].status == checks.FAIL
               and unlabeled["ledger.egress"].detail.startswith(
                   "sink: 1 series whose soak_instance names no instance")
               and "x_0_total" in unlabeled["ledger.egress"].detail,
               f"a series missing the label FAILs, got {unlabeled['ledger.egress']}")
        # Each sink's last queue plus one batch, at its own input's 100-line batch: 200 lines
        # below 0 at the end, not the 400 the two inputs' summed batch would give.
        surplus = {r.id: r for r in checks.run_all(_overlap_run(tmp, "end-surplus",
                                                                end_extra=300))}
        expect(surplus["ledger.overlap"].status == checks.FAIL
               and "= -300, a surplus, past the end allowance 200"
               in surplus["ledger.overlap"].detail,
               f"a surplus at the end past each sink's own batch FAILs, got "
               f"{surplus['ledger.overlap']}")
        expect(surplus["ledger.wire"].status == checks.FAIL
               and "over the end allowance 200" in surplus["ledger.wire"].detail,
               f"ledger.wire FAILs the same surplus, got {surplus['ledger.wire']}")
        stalled = {r.id: r for r in checks.run_all(_overlap_run(tmp, "stalled", stall=True))}
        expect(stalled["progress"].status == checks.FAIL
               and "no sink deliveries in [+120s, +150s): logit-b: hang" in
               stalled["progress"].detail,
               f"a stall of the instance running while the other is stopped FAILs, got "
               f"{stalled['progress']}")


IDP_ROLES = {"logit": "sut", "generator": "generator", "kafka": "broker", "sut-net": "netns",
             "idp": "idp"}
IDP_MODES = {"logit": "service:sut-net", "idp": "service:sut-net"}


def _idp_rules(root):
    """An `idp`'s namespace: the role map's rules (a holder only beside an idp, an idp only
    beside one holder, never with `[overlap]` or a further SUT), `logit` and the idp in the
    holder's namespace, netem and partition on the holder only, no lifecycle fault on it, and
    the fault predicates, under which an idp's lifecycle faults affect egress alone."""
    path = root / "scenarios" / "kafka-oauth-reauth" / "scenario.toml"
    raw = tomllib.loads(path.read_text())
    loaded = scenario.load(path)
    expect(scenario.service_problems(loaded, IDP_ROLES, IDP_MODES) == [],
           f"kafka-oauth-reauth's stack holds, got "
           f"{scenario.service_problems(loaded, IDP_ROLES, IDP_MODES)}")

    def mutate(edit):
        changed = copy.deepcopy(raw)
        edit(changed)
        return changed

    def step(**fields):
        return lambda r: r["step"].append({"for": "20s", **fields})

    def refused(edit, needle, what, roles=IDP_ROLES):
        _refused(path, mutate(edit), needle, what, roles=roles)

    refused(step(at="12m40s", action="netem", on="logit", args="delay 10ms"),
            "logit shares sut-net's network namespace, so put netem on sut-net",
            "netem on the SUT beside an idp")
    refused(step(at="12m40s", action="partition", on="idp"),
            "idp shares sut-net's network namespace, so put partition on sut-net",
            "a partition of the idp")
    refused(step(at="12m40s", action="stop", on="sut-net"),
            "the holder takes netem and partition only; any other fault on it takes logit, "
            "idp's network away", "a stop of the holder")
    refused(lambda r: None, "an idp shares logit's network namespace through a netns holder, "
            "and the stack has none", "an idp with no holder",
            roles={k: v for k, v in IDP_ROLES.items() if k != "sut-net"})
    refused(lambda r: None, "shares logit's namespace through one netns holder, and the stack "
            "has sut-net, sut-net-b", "two holders beside an idp",
            roles={**IDP_ROLES, "sut-net-b": "netns"})
    for edit, what in ((step(at="12m40s", action="netem", on="sut-net", args="delay 10ms"),
                        "netem"),
                       (step(at="12m40s", action="partition", on="sut-net"), "a partition"),
                       (step(at="12m40s", action="restart", on="idp"), "a restart of the idp")):
        try:
            changed = scenario.from_dict(mutate(edit), path)
            scenario.validate(changed)
            expect(scenario.service_problems(changed, IDP_ROLES, IDP_MODES) == [],
                   f"{what} on the holder or the idp is allowed, got "
                   f"{scenario.service_problems(changed, IDP_ROLES, IDP_MODES)}")
        except scenario.ScenarioError as err:
            expect(False, f"{what} on the holder or the idp is refused: {err.problems}")
    for member in ("logit", "idp"):
        problems = scenario.service_problems(loaded, IDP_ROLES, {**IDP_MODES, member: "bridge"})
        expect(any(f'{member} needs network_mode "service:sut-net", so logit reaches idp on '
                   "loopback, and has bridge" in p for p in problems),
               f"{member} outside the holder's namespace is refused, got {problems}")
    overlap_path = root / "scenarios" / "rolling-overlap" / "scenario.toml"
    _refused(overlap_path, tomllib.loads(overlap_path.read_text()),
             "an idp and [overlap] can't be combined", "an idp beside an overlap",
             roles={**OVERLAP_ROLES, "idp": "idp"})
    hop_path = root / "scenarios" / "native-hop" / "scenario.toml"
    _refused(hop_path, tomllib.loads(hop_path.read_text()),
             "an idp's namespace holds logit alone, and the scenario has the SUT services "
             "logit, collector", "an idp beside a further SUT",
             roles={**TWO_SUT_ROLES, "sut-net": "netns", "idp": "idp"})

    for action, on, udp, tcp, egress in (
            ("stop", "idp", False, False, True), ("pause", "idp", False, False, True),
            ("restart", "idp", False, False, True), ("netem", "sut-net", False, True, True),
            ("partition", "sut-net", True, True, True)):
        one = scenario.Step("c0s1", 0, 0, action, on, "", 0, 1)
        got = (faults.ACTIONS[action].affects_udp_ingress(one, IDP_ROLES),
               faults.ACTIONS[action].affects_tcp_ingress(one, IDP_ROLES),
               faults.ACTIONS[action].affects_egress(one, IDP_ROLES))
        expect(tuple(map(bool, got)) == (udp, tcp, egress),
               f"a {action} on {on} beside an idp affects UDP {udp}, TCP {tcp}, egress "
               f"{egress}, got {got}")


def _refresh_stderr_line(offset):
    """A `token_refresh_failed` WARN as `Endpoint::token` logs it for the `kafka` sink."""
    return json.dumps({
        "timestamp": _stamp(T0 + offset), "level": "WARN", "component": "kafka",
        "key": "token_refresh_failed", "target": "logit",
        "message": "refreshing the OAUTHBEARER token from http://127.0.0.1/default/token "
                   "failed; the cached token, valid for another 9.9s, stays in use, with no "
                   "fetch for 9.9s: requesting a token from http://127.0.0.1/default/token: "
                   "error sending request (x1, further occurrences suppressed)"})


def _refresh_checks():
    """`self_log` counts a `token_refresh_failed` line inside a fault window on an `idp`,
    `recovery_bound` included, and WARNs one outside every such window, a broker fault's
    included."""
    timeline = [_phase("start", 0, t0=T0),
                _action("apply", "c0s1", "stop", "kafka", 50),
                _action("revert", "c0s1", "stop", "kafka", 79, 80),
                _action("apply", "c0s2", "stop", "idp", 200),
                _action("revert", "c0s2", "stop", "idp", 319, 320),
                _phase("end_begin", 450), _phase("end_end", 480)]
    services = {**IDP_ROLES}

    def run(tmp, name, offsets):
        run_dir = _run_dir(tmp, name, timeline, services=services)
        (run_dir / "logs" / "logit.stderr").write_text(
            "".join(_refresh_stderr_line(offset) + "\n" for offset in offsets))
        return checks.check_self_log(checks.RunData(run_dir))

    with tempfile.TemporaryDirectory() as tmp:
        inside = run(tmp, "inside", (210, 350))
        expect(inside.status == checks.PASS
               and "2 token_refresh_failed line(s) inside idp faults" in inside.detail,
               f"self_log counts a refresh failure inside an idp stop and its recovery, got "
               f"{inside}")
        outside = run(tmp, "outside", (210, 60))
        expect(outside.status == checks.WARN and "+60s" in outside.detail
               and "outside every fault window on an idp" in outside.detail,
               f"self_log WARNs a refresh failure inside a broker fault only, got {outside}")
        late = run(tmp, "late", (400,))
        expect(late.status == checks.WARN and "+400s" in late.detail,
               f"self_log WARNs a refresh failure past an idp stop's recovery, got {late}")


def _sighup_base(root):
    path = root / "scenarios" / "sighup" / "scenario.toml"
    return path, tomllib.loads(path.read_text())


def _signal_rules(root):
    """Every `signal` and `rename` rule: the keys each takes and refuses, `signal = "HUP"` only,
    on a service that runs `logit`, never inside that service's lifecycle faults, a rename's
    file a stdio_out target in the shared directory of its service's config, its `to` neither a
    target nor another rename's, a SIGHUP starting inside its `for`, once per run, and neither
    in a `[random]` schedule; and `logit.output.file.reopens` read as a counter."""
    path, base = _sighup_base(root)
    loaded = scenario.load(path)
    expect(loaded.uses_shared_dir and not loaded.file_sources,
           "a rename gives the run a shared directory with no file wire")
    expect(scenario.service_problems(loaded, _override_roles(loaded)) == [],
           f"sighup passes its service rules, got "
           f"{scenario.service_problems(loaded, _override_roles(loaded))}")
    expect(scenario.stdio_shared_targets(loaded.config_path("sut").read_text())
           == {"archive.log": "archive"},
           "the SUT's stdio_out in /shared is found by its file name")

    def mutate(edit):
        raw = copy.deepcopy(base)
        edit(raw)
        return raw

    def step(index, **fields):
        def edit(raw):
            raw["step"][index].update(fields)
            for key in [k for k, v in fields.items() if v is None]:
                del raw["step"][index][key]
        return edit

    def add(**fields):
        return lambda raw: raw["step"].append(fields)

    cases = [
        (step(0, signal=None), "signal needs `signal`", "a signal with no signal"),
        (step(0, signal="TERM"), "`signal` must be one of HUP", "a signal other than HUP"),
        (step(0, path="x"), "`path` is for rename only", "path on a signal"),
        (step(4, signal="HUP"), "`signal` is for signal only", "signal on a stop"),
        (step(2, path=None), "rename needs `path`", "a rename with no path"),
        (step(2, to="sub/archive.log.1"), "`to` must be a bare file name", "a to with a slash"),
        (step(2, path=".."), "`path` must be a bare file name", "a path of .."),
        (step(2, path="other.log"), "`path` 'other.log' isn't a stdio_out target",
         "a rename of a file no stdio_out writes"),
        (step(6, to="archive.log.1"), "repeats step 3's", "two renames onto one name"),
        (step(2, args="x"), "`args` is for netem only", "args on a rename"),
        (lambda raw: raw["step"].pop(3), "needs a signal = \"HUP\" step on logit",
         "a rename with no SIGHUP inside it"),
        (step(3, at="2m"), "needs a signal = \"HUP\" step on logit",
         "a SIGHUP starting with its rename"),
        (step(3, at="2m40s"), "needs a signal = \"HUP\" step on logit",
         "a SIGHUP after its rename"),
        (add(at="9m20s", action="stop", on="logit", **{"for": "30s"}),
         "signal on logit during its stop", "a SIGHUP inside its service's stop"),
        (add(at="2m20s", action="pause", on="logit", **{"for": "5s"}),
         "two faults overlap on logit", "a pause inside a rename"),
    ]
    for edit, needle, what in cases:
        _refused(path, mutate(edit), needle, what)
    _refused(path, mutate(step(2, on="generator")), "isn't a stdio_out target in /shared in "
             "generator's config", "a rename on a service with no such target")
    _refused(path, mutate(step(2, on="victoria-metrics")), "victoria-metrics has no config",
             "a rename on a service with no config")
    _refused(path, mutate(step(0, on="victoria-metrics")), "signal is for a service that runs "
             "logit", "a signal on a backend")
    _refused(path, base, "runs 2 times", "a rename in a second cycle",
             duration=scenario.parse_duration("26m"))
    _refused(path, mutate(lambda raw: raw["expect"][0].update(reduce="max")),
             "logit.output.file.reopens is a sum", "a gauge reducer on the reopen counter")
    # The signal inside a rename, and a signal beside a fault on another service, both load.
    expect(scenario.from_dict(copy.deepcopy(base), path).steps[3].signal == "HUP",
           "a SIGHUP inside a rename on its own service loads")

    random_path, random_raw = _random_base(root)
    for action in ("signal", "rename"):
        raw = copy.deepcopy(random_raw)
        raw["random"]["fault"].append({"weight": 1, "action": action, "on": "logit",
                                       "for": {"min": "10s", "max": "20s"}})
        _refused(random_path, raw, f"{action} opens no fault window",
                 f"a {action} in a [random] schedule")


class _SignalDocker:
    """Records each `run` call, as kill_apply's CLI path makes it."""

    def __init__(self):
        self.calls = []

    def run(self, args, timeout=None):
        self.calls.append(list(args))
        return docker.Result(0, "", "")


def _signal_actions():
    """A `signal` sends `docker kill -s HUP` and its revert makes no call; a `rename` moves a
    file in the shared directory and refuses a taken `to`, a missing file, and a run with no
    shared directory; and neither affects any leg for any role."""
    fake = _SignalDocker()
    ctx = faults.Context(docker=fake, ids={"logit": "sut"}, netem_image="netem")
    hup = scenario.Step("c0s1", 0, 0, "signal", "logit", "", 10.0, 20.0, signal="HUP")
    result = faults.ACTIONS["signal"].apply(ctx, hup)
    expect(fake.calls == [["kill", "-s", "HUP", "sut"]] and result["rc"] == 0
           and result["signal"] == "HUP",
           f"a signal is docker kill -s HUP, got {fake.calls} {result}")
    expect(faults.ACTIONS["signal"].revert(ctx, hup) == {"rc": 0, "stderr": ""}
           and len(fake.calls) == 1, "a signal's revert makes no Docker call")
    rename = scenario.Step("c0s2", 1, 0, "rename", "logit", "", 10.0, 40.0, path="a.log",
                           to="a.log.1")
    expect(faults.ACTIONS["rename"].apply(ctx, rename)["rc"] == 1,
           "a rename with no shared directory fails")
    with tempfile.TemporaryDirectory() as tmp:
        ctx.shared_dir = Path(tmp)
        (Path(tmp) / "a.log").write_text("one\n")
        result = faults.ACTIONS["rename"].apply(ctx, rename)
        expect(result["rc"] == 0 and (Path(tmp) / "a.log.1").read_text() == "one\n"
               and not (Path(tmp) / "a.log").exists(),
               f"a rename moves the file on the host, got {result}")
        (Path(tmp) / "a.log").write_text("two\n")
        result = faults.ACTIONS["rename"].apply(ctx, rename)
        expect(result["rc"] == 1 and "already exists" in result["stderr"]
               and (Path(tmp) / "a.log.1").read_text() == "one\n",
               f"a rename never replaces its `to`, got {result}")
        (Path(tmp) / "a.log").unlink()
        (Path(tmp) / "a.log.1").unlink()
        expect(faults.ACTIONS["rename"].apply(ctx, rename)["rc"] == 1,
               "a rename of a missing file fails")
        expect(faults.ACTIONS["rename"].revert(ctx, rename) == {"rc": 0, "stderr": ""},
               "a rename's revert does nothing")
    roles = {**scenario.BASE_ROLES, "kafka": "broker", "standin": "standin", "sut-net": "netns"}
    for name in scenario.NO_WINDOW_ACTIONS:
        action = faults.ACTIONS[name]
        for on in roles:
            one = scenario.Step("x", 0, 0, name, on, "", 0.0, 1.0)
            expect(not action.affects_egress(one, roles)
                   and not any(action.affects(wire, one, roles) for wire in faults.INGRESS_KEYS),
                   f"a {name} on {on} affects no leg")
    expect(not set(scenario.NO_WINDOW_ACTIONS) & set(faults.STARTS_ON_APPLY
                                                      + faults.STARTS_ON_REVERT
                                                      + faults.RESUMES_ON_REVERT),
           "neither a signal nor a rename starts or resumes a container")


def _sighup_driver(root):
    """`prepare()` gives a scenario with a rename, and no file wire, the shared directory, its
    compose variable, and the actions' context."""
    path, _ = _sighup_base(root)
    loaded = scenario.load(path)
    with tempfile.TemporaryDirectory() as tmp:
        run = driver.Run(root, loaded, loaded.duration, None, False, tmp, [], "logit:soak",
                         roles=_override_roles(loaded))
        run.prepare()
        shared = run.run_dir / "shared"
        env = (run.run_dir / "compose.env").read_text()
        expect(shared.is_dir() and (shared.stat().st_mode & 0o777) == 0o777
               and f"SOAK_SHARED_DIR={shared.resolve()}\n" in env
               and run.ctx.shared_dir == shared.resolve(),
               f"prepare() makes the shared directory for a rename, got {env} "
               f"{run.ctx.shared_dir}")
        override = loaded.compose_path().read_text()
        expect(override.count("${SOAK_SHARED_DIR:?") == 1 and "\n  logit:" in override,
               "sighup's override mounts the shared directory into the SUT")
        renames = [s for s in run.steps if s.action == "rename"]
        expect([(s.path, s.to) for s in renames]
               == [("archive.log", "archive.log.1"), ("archive.log", "archive.log.2")]
               and all(s.expected()["path"] == "archive.log" for s in renames),
               f"each rename's names reach its step and its timeline fields, got {renames}")


def _stamp9(t):
    """An RFC 3339 UTC stamp with nine fractional digits, as `format: json` writes it."""
    whole = int(t)
    return (datetime.fromtimestamp(whole, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
            + f".{round((t - whole) * 1e9):09d}Z")


SIGHUP_SUT_CONFIG = ("components:\n"
                     "  statsd: { type: statsd_in, bind: 0.0.0.0:8125 }\n"
                     "  archive:\n"
                     "    type: stdio_out\n"
                     "    sources: [statsd]\n"
                     "    target: /shared/archive.log\n"
                     "    format: json\n")


def _reopen_run(tmp, name, late=False, early=False, extra=0, new_missing=False, kill=False,
                stamp=None):
    """A run directory for `file.reopen`: two renames of archive.log, c0s3 over [+100, +130]
    with its SIGHUP c0s4 applied over [+110, +110.2], and c0s7 over [+200, +230] with c0s8 over
    [+210, +210.2]. archive writes one line a second, each stamped 0.5 s before its write, from
    +60 to +299, into archive.log.1 until the first SIGHUP, archive.log.2 until the second, and
    archive.log after. `late` puts a line stamped +115 in archive.log.1, `early` one stamped
    +105 in archive.log.2, `extra` adds to events.delivered, `new_missing` removes archive.log,
    and `kill` adds a kill of the SUT at +250."""
    run_dir = Path(tmp) / name
    (run_dir / "configs").mkdir(parents=True)
    (run_dir / "shared").mkdir()
    (run_dir / "configs" / "logit-sut.yaml").write_text(SIGHUP_SUT_CONFIG)
    stamp = stamp or _stamp9
    files = {"archive.log.1": [], "archive.log.2": [], "archive.log": []}
    for second in range(60, 300):
        t = T0 + second - 0.5
        name_of = ("archive.log.1" if second <= 110 else "archive.log.2" if second <= 210
                   else "archive.log")
        files[name_of].append(t)
    if late:
        files["archive.log.1"].append(T0 + 115)
    if early:
        files["archive.log.2"].insert(0, T0 + 105)
    for file_name, stamps in files.items():
        if new_missing and file_name == "archive.log":
            continue
        (run_dir / "shared" / file_name).write_text("".join(
            '{"timestamp":"' + stamp(t) + '","metrics":[]}\n' for t in stamps))
    total = sum(len(stamps) for stamps in files.values()) + extra
    stdout = []
    for offset in range(5, 301, 5):
        ts = _stamp(T0 + offset)
        stdout.append(_line(ts, "logit.process.uptime", "gauge", float(offset)))
        if offset == 300:
            stdout.append(_line(ts, "logit.component.events.delivered", "sum", total,
                                component="archive"))
        if offset in (115, 215):
            stdout.append(_line(ts, "logit.output.file.reopens", "sum", 1,
                                component="archive"))
    timeline = [_phase("start", 0, t0=T0)]
    steps = []
    for step_id, action, start, end, extra_fields in (
            ("c0s3", "rename", 100, 130, {"path": "archive.log", "to": "archive.log.1"}),
            ("c0s4", "signal", 110, 120, {"signal": "HUP"}),
            ("c0s7", "rename", 200, 230, {"path": "archive.log", "to": "archive.log.2"}),
            ("c0s8", "signal", 210, 220, {"signal": "HUP"})):
        timeline += [_action("apply", step_id, action, "logit", start, start + 0.2,
                             **extra_fields),
                     _action("revert", step_id, action, "logit", end, end + 0.01)]
        steps.append({"id": step_id, "spec_index": int(step_id[3:]) - 1, "cycle": 0,
                      "action": action, "on": "logit", "args": "", "start": start, "end": end,
                      **extra_fields})
    if kill:
        timeline += [_action("apply", "c0s9", "kill", "logit", 250),
                     _action("revert", "c0s9", "kill", "logit", 260)]
    timeline += [_phase("end_begin", 300), _phase("end_end", 330)]
    expectations = [{"name": "reopen", "service": "logit", "metric": "logit.output.file.reopens",
                     "attrs": {"component": "archive"}, "step": 4, "window": "through",
                     "reduce": "delta", "min": 1, "max": None}]
    run_dir = _run_dir(tmp, name, timeline, stdout, configs={"sut": "logit-sut.yaml"},
                       suts=["logit"], steps=steps, expect=expectations)
    # What signals.rs logs on each SIGHUP, at info, as the sighup run recorded it.
    (run_dir / "logs" / "logit.stderr").write_text("".join(
        f'{{"timestamp":"{_stamp(T0 + at)}","level":"INFO","message":"reopen signal received",'
        f'"generation":{n},"config_reloaded":false,"target":"logit"}}\n'
        for n, at in ((1, 110.1), (2, 210.1))))
    return run_dir


def _reopen_checks():
    """`file.reopen` per rename: a clean run passing both rows, a late line in the renamed
    file, an early line in the new one, a count off by one, and a missing new file each
    FAILing, a kill's count reported; a signal and a rename opening no fault window; and an
    `[[expect]]` through a signal step, and a SIGHUP's info line in `self_log`."""
    with tempfile.TemporaryDirectory() as tmp:
        data = checks.RunData(_reopen_run(tmp, "clean"))
        expect(not data.faults and sorted(f.step for f in data.events)
               == ["c0s3", "c0s4", "c0s7", "c0s8"],
               f"a signal and a rename open no fault window, got {data.faults}")
        rows = {r.id: r for r in checks.check_file_reopen(data)}
        expect(set(rows) == {"file.reopen.c0s3", "file.reopen.c0s7"}
               and all(r.status == checks.PASS for r in rows.values())
               and "lines in archive.log.1, archive.log.2, archive.log: 240, archive's "
                   "events.delivered over logit's lives: 240" in rows["file.reopen.c0s3"].detail,
               f"a clean run passes each rename, got {rows}")
        micro = {r.id: r for r in checks.check_file_reopen(checks.RunData(
            _reopen_run(tmp, "micro", stamp=_stamp)))}
        expect(all(r.status == checks.PASS for r in micro.values()),
               f"six fractional digits parse as nine do, got {micro}")
        self_log = checks.check_self_log(data)
        expect(self_log.status == checks.PASS and len(data.stderr["logit"]) == 2,
               f"a SIGHUP's info line leaves self_log alone, got {self_log}")
        expect(checks.check_expect(data)[0].status == checks.PASS,
               f"an [[expect]] through a signal step reads its window, got "
               f"{checks.check_expect(data)}")
        late = {r.id: r for r in checks.check_file_reopen(checks.RunData(
            _reopen_run(tmp, "late", late=True)))}
        expect(late["file.reopen.c0s3"].status == checks.FAIL
               and "archive.log.1 has a line at +115s, 4.8s after c0s4's SIGHUP finished"
                   in late["file.reopen.c0s3"].detail
               and late["file.reopen.c0s7"].status == checks.PASS,
               f"a line in the renamed file after the signal FAILs, got {late}")
        early = {r.id: r for r in checks.check_file_reopen(checks.RunData(
            _reopen_run(tmp, "early", early=True)))}
        expect(early["file.reopen.c0s3"].status == checks.FAIL
               and "archive.log.2 has a line at +105s, 5.0s before c0s4's SIGHUP started"
                   in early["file.reopen.c0s3"].detail,
               f"a line in the new file from before the signal FAILs, got {early}")
        off = {r.id: r for r in checks.check_file_reopen(checks.RunData(
            _reopen_run(tmp, "off", extra=1)))}
        expect(all(r.status == checks.FAIL and "off by -1" in r.detail for r in off.values()),
               f"lines short of events.delivered FAIL, got {off}")
        missing = {r.id: r for r in checks.check_file_reopen(checks.RunData(
            _reopen_run(tmp, "missing", new_missing=True)))}
        expect(missing["file.reopen.c0s7"].status == checks.FAIL
               and "the new file archive.log is missing" in missing["file.reopen.c0s7"].detail
               and missing["file.reopen.c0s3"].status == checks.FAIL,
               f"a missing new file FAILs, got {missing}")
        killed = {r.id: r for r in checks.check_file_reopen(checks.RunData(
            _reopen_run(tmp, "killed", extra=3, kill=True)))}
        expect(all(r.status == checks.PASS and "reported, not judged: c0s9" in r.detail
                   for r in killed.values()),
               f"a killed life's count is reported, got {killed}")
        pruned_dir = _reopen_run(tmp, "pruned")
        counts = {name: checks.ndjson_file_lines(pruned_dir / "shared" / name)
                  for name in ("archive.log.1", "archive.log.2")}
        (pruned_dir / checks.SHARED_COUNTS).write_text(json.dumps(counts))
        for name in counts:
            (pruned_dir / "shared" / name).unlink()
        pruned = {r.id: r for r in checks.check_file_reopen(checks.RunData(pruned_dir))}
        expect(all(r.status == checks.PASS for r in pruned.values()),
               f"deleted files are read from the recorded counts, got {pruned}")
        (pruned_dir / "shared" / "archive.log").unlink()
        pruned = {r.id: r for r in checks.check_file_reopen(checks.RunData(pruned_dir))}
        expect(pruned["file.reopen.c0s7"].status == checks.FAIL
               and "the new file archive.log is missing" in pruned["file.reopen.c0s7"].detail,
               f"a file neither present nor recorded is missing, got {pruned}")
        none = checks.check_file_reopen(checks.RunData(_run_dir(tmp, "none",
                                                                 [_phase("start", 0, t0=T0)])))
        expect(len(none) == 1 and none[0].status == checks.SKIP,
               f"a run with no rename SKIPs, got {none}")


def _tls_base(root):
    path = root / "scenarios" / "tls-reload" / "scenario.toml"
    return path, tomllib.loads(path.read_text())


def _tls_rules(root):
    """Every `[tls]`, `rotate`, and `probe` rule: the table's keys, names, DNS names, and
    usages; the keys each step takes and refuses; a rotate's certificate read by its service's
    config, its days above the first issue's and every earlier rotation's, and once per run; a
    probe of a server certificate inside a rotate of it on its own service, after a SIGHUP
    there, never inside the service's lifecycle faults; neither in a `[random]` schedule; and
    the `rise` reducer read against a gauge only."""
    path, base = _tls_base(root)
    loaded = scenario.load(path)
    expect(loaded.uses_tls_dir and not loaded.uses_shared_dir
           and loaded.tls == {"statsd": {"san": ["logit"], "usage": "server"},
                              "generator": {"san": ["generator"], "usage": "client"}}
           and loaded.to_json()["tls"] == loaded.tls,
           f"tls-reload's [tls] table loads and reaches the resolved scenario, got {loaded.tls}")
    expect(scenario.service_problems(loaded, _override_roles(loaded)) == [],
           f"tls-reload passes its service rules, got "
           f"{scenario.service_problems(loaded, _override_roles(loaded))}")
    probes = [s for s in scenario.expand(loaded) if s.action == "probe"]
    expect([(s.cert, s.port) for s in probes] == [("statsd", 8125)] * 4
           and probes[0].expected() == {"cert": "statsd", "port": 8125},
           f"each probe's certificate and port reach its step and its timeline fields, got "
           f"{probes}")
    rotates = [s for s in scenario.expand(loaded) if s.action == "rotate"]
    expect([(s.cert, s.days, s.broken) for s in rotates]
           == [("statsd", 31, None), ("generator", 31, None), ("statsd", 32, True),
               ("statsd", 33, None), ("statsd", 34, None)],
           f"each rotation's certificate, days, and broken flag reach its step, got {rotates}")

    def mutate(edit):
        raw = copy.deepcopy(base)
        edit(raw)
        return raw

    def step(index, **fields):
        def edit(raw):
            raw["step"][index].update(fields)
            for key in [k for k, v in fields.items() if v is None]:
                del raw["step"][index][key]
        return edit

    def cert(name, **fields):
        def edit(raw):
            raw["tls"]["cert"][name].update(fields)
        return edit

    def add(**fields):
        return lambda raw: raw["step"].append(fields)

    cases = [
        (lambda raw: raw.update(tls="x"), "`tls` must be a table", "a [tls] that isn't a table"),
        (lambda raw: raw["tls"].update(ca="x"), "tls: unknown key `ca`", "an unknown [tls] key"),
        (lambda raw: raw["tls"].update(cert={}), "needs one [tls.cert.<name>] table or more",
         "a [tls] with no certificate"),
        (lambda raw: raw["tls"]["cert"].update(probe={"san": ["p"], "usage": "client"}),
         "isn't `probe`", "a certificate named as the probe's own"),
        (lambda raw: raw["tls"]["cert"].update(Bad={"san": ["p"], "usage": "client"}),
         "a certificate's name is lowercase", "a certificate name that isn't a slug"),
        (cert("statsd", days=30), "tls.cert.statsd: unknown key `days`",
         "an unknown certificate key"),
        (cert("statsd", san=[]), "`san` must be a list of one DNS name or more",
         "an empty san"),
        (cert("statsd", san="logit"), "`san` must be a list", "a san that isn't a list"),
        (cert("statsd", san=["Logit_1"]), "`san` must be a list", "a san that isn't a DNS name"),
        (cert("statsd", usage="both"), "`usage` must be one of server, client", "a bad usage"),
        (lambda raw: raw.pop("tls"), "rotate needs a [tls] table", "a rotate with no [tls]"),
        (step(0, cert=None), "rotate needs `cert`", "a rotate with no cert"),
        (step(0, cert="other"), "`cert` 'other' names no [tls.cert] table",
         "a rotate of an unknown certificate"),
        (step(0, days=None), "rotate needs `days`", "a rotate with no days"),
        (step(0, days=30), "`days` must be a whole number above the first issue's 30",
         "a rotate no longer than the first issue"),
        (step(0, days="31"), "`days` must be a whole number", "days as a string"),
        (step(0, days=True), "`days` must be a whole number", "days as a boolean"),
        (step(0, broken="yes"), "`broken` must be true or false", "broken as a string"),
        (step(1, cert="statsd"), "`cert` is for rotate and probe only", "cert on a signal"),
        (step(1, days=31), "`days` is for rotate only", "days on a signal"),
        (step(4, broken=True), "`broken` is for rotate only", "broken on a restart"),
        (step(0, port=8125), "`port` is for probe only", "port on a rotate"),
        (step(2, port=None), "probe needs `port`", "a probe with no port"),
        (step(2, port=0), "`port` must be a port number", "port 0"),
        (step(2, port=70000), "`port` must be a port number", "a port past 65535"),
        (step(2, cert="generator"), "and generator is a client one",
         "a probe of a client certificate"),
        (step(2, args="x"), "`args` is for netem only", "args on a probe"),
        (step(0, on="generator"), "generator's config reads nothing under /tls/statsd/live/",
         "a rotate on a service whose config doesn't read it"),
        (step(8, days=32), "`days` 32 must be above the earlier rotation's 32",
         "a rotation no longer than the one before it"),
        (step(2, at="45s"), "must start inside a rotate of statsd on logit",
         "a probe after its rotation"),
        (step(1, at="27s"), "needs a signal = \"HUP\" step on logit starting inside step 1's "
         "rotate and before the probe", "a probe before its SIGHUP"),
        (add(at="3m10s", action="probe", on="logit", cert="statsd", port=8125,
             **{"for": "5s"}), "probe on logit during its restart",
         "a probe inside its service's restart"),
        (add(at="20s", action="rotate", on="logit", cert="statsd", days=40,
             **{"for": "30s"}), "two faults overlap on logit", "two rotations overlapping"),
        (lambda raw: raw["expect"][0].update(reduce="rise"),
         "reduce `rise` reads a gauge, and logit.tls.reloads is a sum",
         "rise on a counter"),
    ]
    for edit, needle, what in cases:
        _refused(path, mutate(edit), needle, what, roles=_override_roles(loaded))
    _refused(path, mutate(step(0, on="victoria-metrics")), "victoria-metrics has no config",
             "a rotate on a service with no config", roles=_override_roles(loaded))
    # The loader refuses a probe on a service with no rotation first, so the service rule is
    # checked on a loaded scenario's step directly.
    moved = scenario.load(path)
    moved.steps[2].on = "victoria-metrics"
    problems = scenario.service_problems(moved, _override_roles(moved))
    expect(any("probe is for a service that runs logit" in p for p in problems),
           f"a probe on a backend is refused, got {problems}")
    _refused(path, base, "runs 2 times", "a rotation in a second cycle",
             duration=scenario.parse_duration("28m"), roles=_override_roles(loaded))
    # A rotate whose service reads the file only in a comment reaches no process.
    with tempfile.TemporaryDirectory() as tmp:
        for name in ("scenario.toml", "logit-generator.yaml", "compose.override.yaml"):
            shutil.copy(path.parent / name, Path(tmp) / name)
        sut = (path.parent / "logit-sut.yaml").read_text()
        (Path(tmp) / "logit-sut.yaml").write_text(
            sut.replace("cert_file: /tls/statsd/live/cert.pem", "cert_file: /tls/x.pem")
            .replace("key_file: /tls/statsd/live/key.pem", "key_file: /tls/y.pem")
            + "# /tls/statsd/live/cert.pem\n")
        _refused(Path(tmp) / "scenario.toml", copy.deepcopy(base),
                 "logit's config reads nothing under /tls/statsd/live/",
                 "a rotate whose path appears only in a comment", roles=_override_roles(loaded))

    # A rotation a later probe reads must be loaded on a SUT that doesn't poll: by a SIGHUP
    # inside it, or by a start of the SUT before the certificate's next rotation.
    def extra_rotation(at, signal_at=None):
        def edit(raw):
            for index, days in ((5, 40), (8, 41), (12, 42)):
                raw["step"][index]["days"] = days
            raw["step"].append({"at": at, "action": "rotate", "on": "logit", "cert": "statsd",
                                "days": 32, "for": "20s"})
            if signal_at is not None:
                raw["step"].append({"at": signal_at, "action": "signal", "on": "logit",
                                    "signal": "HUP", "for": "5s"})
        return edit

    def loads(raw, what, at=path):
        try:
            scenario.validate(scenario.from_dict(raw, at))
            expect(True, "")
        except scenario.ScenarioError as err:
            expect(False, f"{what}: refused with {err.problems}")

    unloaded = mutate(extra_rotation("3m40s"))
    _refused(path, unloaded, "logit polls no TLS files (tls_reload_interval: 0s), and nothing "
             "loads this rotation of statsd before a later probe reads it",
             "a rotation no SIGHUP or start loads, on a SUT that doesn't poll",
             roles=_override_roles(loaded))
    loads(mutate(extra_rotation("1m")), "a rotation the SUT's restart loads before the next one")
    loads(mutate(extra_rotation("3m40s", signal_at="3m45s")),
          "a rotation a SIGHUP inside it loads")
    with tempfile.TemporaryDirectory() as tmp:
        for name in ("scenario.toml", "logit-generator.yaml", "compose.override.yaml"):
            shutil.copy(path.parent / name, Path(tmp) / name)
        (Path(tmp) / "logit-sut.yaml").write_text(
            (path.parent / "logit-sut.yaml").read_text().replace(
                "tls_reload_interval: 0s", "tls_reload_interval: 10s"))
        loads(copy.deepcopy(unloaded), "a rotation on a SUT that polls its files",
              at=Path(tmp) / "scenario.toml")
    expect(not scenario.tls_reload_polls("tls_reload_interval: 0s\n")
           and not scenario.tls_reload_polls("tls_reload_interval: \"0ms\"  # off\n")
           and scenario.tls_reload_polls("tls_reload_interval: 10s\n")
           and scenario.tls_reload_polls("components: {}\n")
           and scenario.tls_reload_polls("# tls_reload_interval: 0s\ncomponents: {}\n"),
           "a config polls unless its top-level tls_reload_interval is zero")

    random_path, random_raw = _random_base(root)
    for action in ("rotate", "probe"):
        raw = copy.deepcopy(random_raw)
        raw["random"]["fault"].append({"weight": 1, "action": action, "on": "logit",
                                       "for": {"min": "10s", "max": "20s"}})
        _refused(random_path, raw, f"{action} opens no fault window",
                 f"a {action} in a [random] schedule")


class _CertsDocker:
    """Records each `run_oneshot` call and answers with the next of `answers`, a list of
    `docker.Result`s, or a report naming the serial `S<n>` for the n-th call."""

    def __init__(self, answers=None):
        self.calls = []
        self.answers = list(answers or [])

    def run_oneshot(self, args, timeout=None):
        self.calls.append(list(args))
        if self.answers:
            return self.answers.pop(0)
        return docker.Result(0, "Generating...\n" + json.dumps(
            {"serial": f"S{len(self.calls)}", "not_after": 1000 + len(self.calls)}) + "\n", "")

    def __getattr__(self, name):
        raise AssertionError(f"_CertsDocker.{name} called")


def _tls_actions():
    """The pinned certs.sh command lines: a `rotate` issues the next generation as the host user
    into `<run>/tls`, with `--broken` when asked, and records its serial, notAfter, and
    generation; a `probe` joins the target's namespace with the directory read-only and records
    what was served and whether it verified; a run of certs.sh with no report, a failed one,
    and an unknown certificate each fail the step."""
    certs = {"statsd": {"san": ["logit", "logit.local"], "usage": "server"},
             "generator": {"san": ["generator"], "usage": "client"}}
    fake = _CertsDocker()
    ctx = faults.Context(docker=fake, ids={"logit": "sut"}, netem_image="netem",
                         tls_dir="/run/tls", tls_certs=certs, user="1000:1001",
                         state={faults.TLS_GEN_KEY: {"statsd": 1, "generator": 1}})
    rotate = scenario.Step("c0s1", 0, 0, "rotate", "logit", "", 60.0, 100.0, cert="statsd",
                           days=31)
    result = faults.ACTIONS["rotate"].apply(ctx, rotate)
    expect(fake.calls[-1] == ["--user", "1000:1001", "-v", "/run/tls:/tls:z", "--entrypoint",
                              "/certs.sh", "netem", "issue", "statsd", "2", "31", "server",
                              "logit", "logit.local"]
           and result == {"rc": 0, "stderr": "", "serial": "S1", "not_after": 1001, "gen": 2},
           f"a rotate issues the next generation as the host user, got {fake.calls} {result}")
    broken = scenario.Step("c0s6", 5, 0, "rotate", "logit", "", 300.0, 340.0, cert="statsd",
                           days=32, broken=True)
    result = faults.ACTIONS["rotate"].apply(ctx, broken)
    expect(fake.calls[-1][7:] == ["issue", "statsd", "3", "32", "server", "logit",
                                  "logit.local", "--broken"]
           and result["gen"] == 3,
           f"a broken rotate passes --broken, got {fake.calls[-1]} {result}")
    expect(faults.ACTIONS["rotate"].revert(ctx, rotate) == {"rc": 0, "stderr": ""}
           and len(fake.calls) == 2, "a rotate's revert makes no Docker call")
    probe = scenario.Step("c0s3", 2, 0, "probe", "logit", "", 85.0, 90.0, cert="statsd",
                          port=8125)
    fake.answers = [docker.Result(0, json.dumps({"serial": "S1", "not_after": 1001,
                                                 "verified": True}) + "\n", "")]
    result = faults.ACTIONS["probe"].apply(ctx, probe)
    expect(fake.calls[-1] == ["--network", "container:sut", "--user", "1000:1001", "-v",
                              "/run/tls:/tls:ro,z", "--entrypoint", "/certs.sh", "netem",
                              "probe", "8125", "logit"]
           and result == {"rc": 0, "stderr": "", "serial": "S1", "not_after": 1001,
                          "verified": True},
           f"a probe joins the target's namespace and reads the first DNS name, got "
           f"{fake.calls[-1]} {result}")
    fake.answers = [docker.Result(0, "no report\n", "")]
    result = faults.ACTIONS["probe"].apply(ctx, probe)
    expect(result["rc"] == 1 and "printed no report" in result["stderr"],
           f"certs.sh with no report fails the step, got {result}")
    fake.answers = [docker.Result(1, "", "127.0.0.1:8125 served no certificate")]
    result = faults.ACTIONS["probe"].apply(ctx, probe)
    expect(result["rc"] == 1 and "served no certificate" in result["stderr"],
           f"a failed probe keeps its rc and stderr, got {result}")
    fake.answers = [docker.Result(2, "", "openssl: error")]
    result = faults.ACTIONS["rotate"].apply(ctx, rotate)
    expect(result["rc"] == 2 and result["gen"] == 4
           and ctx.state[faults.TLS_GEN_KEY]["statsd"] == 4,
           f"a failed issue still uses its generation up, got {result} {ctx.state}")
    calls = len(fake.calls)
    unknown = scenario.Step("x", 0, 0, "rotate", "logit", "", 0.0, 1.0, cert="other", days=31)
    expect(faults.ACTIONS["rotate"].apply(ctx, unknown)["rc"] == 1
           and faults.ACTIONS["probe"].apply(ctx, unknown)["rc"] == 1
           and len(fake.calls) == calls,
           "a rotate or probe of a certificate the run doesn't have fails with no call")
    expect(faults.cert_report(docker.Result(0, "{\"not\": 1}\n", "")) is None
           and faults.cert_report(docker.Result(0, "", "")) is None,
           "a report needs a serial")


def _tls_driver(root):
    """`prepare()` for a scenario with a `[tls]` table: `<run>/tls` and its compose variable,
    certs.sh's `init` and then each certificate's first issue, pinned, the `tls_issued` phase,
    the generations the rotations count from, and the actions' context; a certs.sh that fails
    stops `prepare()`. The override mounts the directory into both `logit` services."""
    path, _ = _tls_base(root)
    loaded = scenario.load(path)
    original = driver.Docker
    try:
        with tempfile.TemporaryDirectory() as tmp:
            made = []

            def fake(*args, answers=None, **kwargs):
                made.append(_CertsDocker(answers))
                return made[-1]

            driver.Docker = fake
            run = driver.Run(root, loaded, loaded.duration, None, False, tmp, [], "logit:soak",
                             roles=_override_roles(loaded))
            # A narrow umask, as a host can set: the directory must still be 0755.
            umask = os.umask(0o077)
            try:
                run.prepare()
            finally:
                os.umask(umask)
            tls = (run.run_dir / "tls").resolve()
            env = (run.run_dir / "compose.env").read_text()
            expect((tls.stat().st_mode & 0o777) == 0o755,
                   f"the TLS directory is 0755 whatever the umask, got {tls.stat().st_mode:o}")
            user = f"{os.getuid()}:{os.getgid()}"
            head = ["--user", user, "-v", f"{tls}:/tls:z", "--entrypoint", "/certs.sh",
                    driver.NETEM_IMAGE]
            expect(tls.is_dir() and f"SOAK_TLS_DIR={tls}\n" in env and run.ctx.tls_dir == tls
                   and run.ctx.user == user and run.ctx.tls_certs == loaded.tls,
                   f"prepare() makes the TLS directory and names it, got {env} {run.ctx}")
            expect(made[0].calls == [head + ["init"],
                                     head + ["issue", "generator", "1", "30", "client",
                                             "generator"],
                                     head + ["issue", "statsd", "1", "30", "server", "logit"]],
                   f"prepare() makes the CA, then each certificate's first issue, got "
                   f"{made[0].calls}")
            for jsonl in (run.timeline, run.watchdog, run.stats, run.freshness, run.capture):
                jsonl.close()
            issued = [r for r in collect.read_jsonl(run.run_dir / "timeline.jsonl")
                      if r.get("phase") == "tls_issued"]
            expect(len(issued) == 1 and issued[0]["certs"] == {
                       "generator": {"gen": 1, "days": 30, "serial": "S2", "not_after": 1002},
                       "statsd": {"gen": 1, "days": 30, "serial": "S3", "not_after": 1003}}
                   and run.ctx.state[faults.TLS_GEN_KEY] == {"generator": 1, "statsd": 1},
                   f"prepare() records the first issues and counts generations from 1, got "
                   f"{issued} {run.ctx.state}")

            driver.Docker = lambda *args, **kwargs: _CertsDocker(
                [docker.Result(0, "{}\n", ""), docker.Result(1, "", "unknown command")])
            stale = driver.Run(root, loaded, loaded.duration, None, False, Path(tmp) / "stale",
                               [], "logit:soak", roles=_override_roles(loaded))
            try:
                stale.prepare()
                expect(False, "prepare() went on after certs.sh printed no report")
            except scenario.ScenarioError as err:
                expect("certs.sh init failed (rc 0)" in str(err)
                       and "rebuild it without SOAK_SKIP_IMAGE" in str(err),
                       f"prepare() stops when certs.sh fails, got {err}")
            for jsonl in (stale.timeline, stale.watchdog, stale.stats, stale.freshness,
                          stale.capture):
                jsonl.close()
    finally:
        driver.Docker = original
    override = loaded.compose_path().read_text()
    expect(override.count("${SOAK_TLS_DIR:?set by script/soak}:/tls:ro,z") == 2
           and "\n  logit:" in override and "\n  generator:" in override,
           "tls-reload's override mounts the TLS directory read-only into both logit services")
    netem = (root / "netem" / "Dockerfile").read_text()
    expect("openssl" in netem and "COPY certs.sh /certs.sh" in netem
           and faults.CERTS_ENTRYPOINT == "/certs.sh",
           "the netem image carries openssl and certs.sh at the entrypoint faults.py names")
    script = (root / "netem" / "certs.sh").read_text()
    expect("ec_paramgen_curve:P-256" in script and "mv -T" in script
           and 'ln -sfn "gen-${gen}"' in script and "chmod 0644" in script
           and "-cert probe/cert.pem" in script,
           "certs.sh makes P-256 keys, swaps a relative live link in one rename, makes its "
           "files readable, and presents the probe's certificate")


def _tls_stderr_line(offset, key):
    """A stderr line as `Watched::check` logs it for the generator's `statsd` sink: a
    `tls_reload_failed` WARN or a `tls_reloaded` INFO."""
    if key == "tls_reloaded":
        level, message = "INFO", ("reloaded TLS files tls.ca_file /tls/ca.pem, tls.cert_file "
                                  "/tls/generator/live/cert.pem, tls.key_file "
                                  "/tls/generator/live/key.pem")
    else:
        level, message = "WARN", ("TLS reload failed, still using the previous files: loading "
                                  "tls.cert_file /tls/statsd/live/cert.pem with tls.key_file "
                                  "/tls/statsd/live/key.pem: keys may not be consistent: "
                                  "KeyMismatch (x1, further occurrences suppressed)")
    return json.dumps({"timestamp": _stamp(T0 + offset), "level": level, "message": message,
                       "component": "statsd", "key": key, "target": "logit"}) + "\n"


def _tls_run(tmp, name, served="B2", verified=True, issued=True, first_broken=False,
             failed_probe=False, reload_failed_at=None, failed_on="logit", lines=()):
    """A run directory for `tls.served`, `self_log`, and `rise`: statsd first issued as A1
    (notAfter 100), rotated at c0s1 [+100, +140] to gen 2, B2 (notAfter 200), with a SIGHUP
    c0s2 [+110, +120] and a probe c0s3 [+125, +130]; the generator's certificate rotated at
    c0s4 [+150, +190]; then statsd rotated broken at c0s6 [+200, +240] to gen 3, C3, with a
    SIGHUP c0s7 and a probe c0s8 [+225, +230] that read `served`. The SUT's config sets
    `tls_reload_interval: 0s` and the generator's `10s`. `first_broken` makes c0s1 the broken
    one, `issued` False leaves out the `tls_issued` phase, `failed_probe` fails c0s8's apply,
    `reload_failed_at` puts a `tls_reload_failed` line on `failed_on`'s stderr at that offset,
    and `lines` adds `(service, offset, key)` lines. The SUT's not_after gauge reads 100 until
    +115 and 200 after it."""
    timeline = [_phase("start", 0, t0=T0)]
    if issued:
        timeline.append(_phase("tls_issued", -5, certs={"statsd": {
            "gen": 1, "days": 30, "serial": "A1", "not_after": 100}}))
    steps = []
    for step_id, action, start, end, fields in (
            ("c0s1", "rotate", 100, 140, {"cert": "statsd", "days": 31, "gen": 2,
                                          "serial": "B2", "not_after": 200,
                                          **({"broken": True} if first_broken else {})}),
            ("c0s2", "signal", 110, 120, {"signal": "HUP"}),
            ("c0s3", "probe", 125, 130, {"cert": "statsd", "port": 8125,
                                         "serial": "A1" if first_broken else "B2",
                                         "not_after": 100 if first_broken else 200,
                                         "verified": True}),
            ("c0s4", "rotate", 150, 190, {"cert": "generator", "days": 31, "gen": 2,
                                          "serial": "G2", "not_after": 250}),
            ("c0s6", "rotate", 200, 240, {"cert": "statsd", "days": 32, "gen": 3,
                                          "serial": "C3", "not_after": 300, "broken": True}),
            ("c0s7", "signal", 210, 220, {"signal": "HUP"}),
            ("c0s8", "probe", 225, 230, {"cert": "statsd", "port": 8125, "serial": served,
                                         "not_after": {"B2": 200, "C3": 300, "A1": 100}[served],
                                         "verified": verified})):
        rc = 1 if failed_probe and step_id == "c0s8" else 0
        apply_fields = dict(fields)
        if rc:
            apply_fields = {"cert": "statsd", "port": 8125, "stderr": "served no certificate"}
        on = "generator" if step_id == "c0s4" else "logit"
        timeline += [_action("apply", step_id, action, on, start, start + 0.5, rc=rc,
                             **apply_fields),
                     _action("revert", step_id, action, on, end, end + 0.01)]
        steps.append({"id": step_id, "spec_index": int(step_id[3:]) - 1, "cycle": 0,
                      "action": action, "on": on, "args": "", "start": start, "end": end})
    timeline += [_phase("end_begin", 300), _phase("end_end", 330)]
    stdout = []
    for offset in range(5, 301, 5):
        ts = _stamp(T0 + offset)
        stdout.append(_line(ts, "logit.process.uptime", "gauge", float(offset)))
        stdout.append(_line(ts, "logit.tls.certificate.not_after", "gauge",
                            100.0 if offset <= 115 else 200.0, component="statsd",
                            attrs={"side": "server"}))
    expectations = [
        {"name": "rises", "service": "logit", "metric": "logit.tls.certificate.not_after",
         "attrs": {"component": "statsd", "side": "server"}, "step": 2, "window": "through",
         "reduce": "rise", "min": 86, "max": 100},
        {"name": "holds", "service": "logit", "metric": "logit.tls.certificate.not_after",
         "attrs": {"component": "statsd", "side": "server"}, "step": 7, "window": "through",
         "reduce": "rise", "min": 0, "max": 0},
        {"name": "too-little", "service": "logit", "metric": "logit.tls.certificate.not_after",
         "attrs": {"component": "statsd", "side": "server"}, "step": 2, "window": "through",
         "reduce": "rise", "min": 86400},
        {"name": "before-any", "service": "logit", "metric": "logit.tls.certificate.not_after",
         "attrs": {"component": "statsd", "side": "client"}, "step": 2, "window": "through",
         "reduce": "rise", "min": 0}]
    run_dir = _run_dir(tmp, name, timeline, stdout, suts=["logit"], steps=steps,
                       expect=expectations,
                       configs={"sut": "logit-sut.yaml", "generator": "logit-generator.yaml"})
    (run_dir / "configs").mkdir()
    (run_dir / "configs" / "logit-sut.yaml").write_text(
        "tls_reload_interval: 0s\ncomponents: {}\n")
    (run_dir / "configs" / "logit-generator.yaml").write_text(
        "tls_reload_interval: 10s\ncomponents: {}\n")
    by_service = {}
    if reload_failed_at is not None:
        by_service.setdefault(failed_on, []).append((reload_failed_at, "tls_reload_failed"))
    for service, offset, key in lines:
        by_service.setdefault(service, []).append((offset, key))
    for service, entries in by_service.items():
        (run_dir / "logs" / f"{service}.stderr").write_text("".join(
            _tls_stderr_line(offset, key) for offset, key in sorted(entries)))
    return run_dir


def _tls_checks():
    """`tls.served` per probe: the generation in force passing, a broken rotation's serial
    served FAILing, a chain that didn't verify FAILing, the first issue in force after a broken
    first rotation, no recorded first issue FAILing, a failed probe FAILing, and no probe
    SKIPping; `self_log` excusing `tls_reload_failed` inside a broken rotation on its service
    only; the four steps opening no fault window; and the `rise` reducer."""
    with tempfile.TemporaryDirectory() as tmp:
        data = checks.RunData(_tls_run(tmp, "clean"))
        expect(not data.faults and sorted(f.step for f in data.events)
               == ["c0s1", "c0s2", "c0s3", "c0s4", "c0s6", "c0s7", "c0s8"],
               f"a rotate and a probe open no fault window, got {data.faults}")
        rows = {r.id: r for r in checks.check_tls_served(data)}
        expect(set(rows) == {"tls.served.c0s3", "tls.served.c0s8"}
               and all(r.status == checks.PASS for r in rows.values())
               and "want c0s1's gen 2, serial B2, notAfter 200" in rows["tls.served.c0s3"].detail
               and "still in force after broken c0s6" in rows["tls.served.c0s8"].detail,
               f"each probe reads the generation in force, got {rows}")
        bad = {r.id: r for r in checks.check_tls_served(checks.RunData(
            _tls_run(tmp, "bad", served="C3")))}
        expect(bad["tls.served.c0s8"].status == checks.FAIL
               and "serial differs; notAfter differs" in bad["tls.served.c0s8"].detail
               and bad["tls.served.c0s3"].status == checks.PASS,
               f"a broken rotation's certificate served FAILs, got {bad}")
        unverified = {r.id: r for r in checks.check_tls_served(checks.RunData(
            _tls_run(tmp, "unverified", verified=False)))}
        expect(unverified["tls.served.c0s8"].status == checks.FAIL
               and "didn't verify" in unverified["tls.served.c0s8"].detail,
               f"a chain that didn't verify FAILs, got {unverified}")
        first = {r.id: r for r in checks.check_tls_served(checks.RunData(
            _tls_run(tmp, "first", served="A1", first_broken=True)))}
        expect(all(r.status == checks.PASS for r in first.values())
               and "want the first issue (gen 1), serial A1" in first["tls.served.c0s8"].detail,
               f"the first issue stays in force through broken rotations, got {first}")
        unissued = {r.id: r for r in checks.check_tls_served(checks.RunData(
            _tls_run(tmp, "unissued", served="A1", first_broken=True, issued=False)))}
        expect(all(r.status == checks.FAIL and "no first issue" in r.detail
                   for r in unissued.values()),
               f"a probe with no recorded first issue FAILs, got {unissued}")
        failed = {r.id: r for r in checks.check_tls_served(checks.RunData(
            _tls_run(tmp, "failed", failed_probe=True)))}
        expect(failed["tls.served.c0s8"].status == checks.FAIL
               and "returned rc 1: served no certificate" in failed["tls.served.c0s8"].detail,
               f"a probe that failed FAILs its row, got {failed}")
        none = checks.check_tls_served(checks.RunData(_run_dir(tmp, "none",
                                                                [_phase("start", 0, t0=T0)])))
        expect(len(none) == 1 and none[0].status == checks.SKIP,
               f"a run with no probe SKIPs, got {none}")

        inside = checks.check_self_log(checks.RunData(
            _tls_run(tmp, "inside", reload_failed_at=210.2)))
        expect(inside.status == checks.PASS
               and "1 tls_reload_failed line(s) inside broken rotations" in inside.detail,
               f"a failed reload inside a broken rotation is expected, got {inside}")
        outside = checks.check_self_log(checks.RunData(
            _tls_run(tmp, "outside", reload_failed_at=110.2)))
        expect(outside.status == checks.FAIL
               and "tls_reload_failed at +110s, outside every broken rotate on logit"
                   in outside.detail,
               f"a failed reload outside every broken rotation FAILs, got {outside}")
        elsewhere = checks.check_self_log(checks.RunData(
            _tls_run(tmp, "elsewhere", reload_failed_at=210.2, failed_on="generator")))
        expect(elsewhere.status == checks.FAIL
               and "outside every broken rotate on generator" in elsewhere.detail,
               f"a broken rotation on one service excuses no other's, got {elsewhere}")
        torn = checks.check_self_log(checks.RunData(_tls_run(tmp, "torn", lines=(
            ("generator", 160.2, "tls_reload_failed"), ("generator", 170.2, "tls_reloaded")))))
        expect(torn.status == checks.PASS
               and "1 torn read(s) a later reload in their rotation replaced" in torn.detail,
               f"a polling service's failed reload that a reload follows in its rotate is a "
               f"torn read, got {torn}")
        unreplaced = checks.check_self_log(checks.RunData(_tls_run(tmp, "unreplaced", lines=(
            ("generator", 160.2, "tls_reload_failed"), ("generator", 195.2, "tls_reloaded")))))
        expect(unreplaced.status == checks.FAIL
               and "tls_reload_failed at +160s, outside every broken rotate on generator and "
                   "with no reload after it in its rotate" in unreplaced.detail,
               f"a failed reload with no later reload in its rotate FAILs, got {unreplaced}")
        unpolled = checks.check_self_log(checks.RunData(_tls_run(tmp, "unpolled", lines=(
            ("logit", 110.2, "tls_reload_failed"), ("logit", 112.2, "tls_reloaded")))))
        expect(unpolled.status == checks.FAIL
               and "tls_reload_failed at +110s, outside every broken rotate on logit"
                   in unpolled.detail,
               f"a failed reload on a service that doesn't poll FAILs, reload or not, got "
               f"{unpolled}")

        rows = {r.id: r for r in checks.check_expect(data)}
        expect(rows["expect.rises"].status == checks.PASS
               and "c0s2 (+110s, +165s]: 100 " in rows["expect.rises"].detail,
               f"rise is the value at the window's end less the one in force at its start, got "
               f"{rows['expect.rises']}")
        expect(rows["expect.holds"].status == checks.PASS
               and ": 0 (" in rows["expect.holds"].detail,
               f"a gauge that holds rises 0, got {rows['expect.holds']}")
        expect(rows["expect.too-little"].status == checks.FAIL,
               f"a rise under its bound FAILs, got {rows['expect.too-little']}")
        expect(rows["expect.before-any"].status == checks.FAIL
               and "no sample by the window's end" in rows["expect.before-any"].detail,
               f"a gauge never set FAILs rise, got {rows['expect.before-any']}")
        tel = data.telemetry["logit"]
        value, note = checks.reduce_gauge(tel, "logit.tls.certificate.not_after",
                                          {"component": "statsd"}, T0 + 2, T0 + 30, "rise")
        expect(value is None and "no value in force at the window's start" in note,
               f"rise with no value in force at the start is None, got {value} {note}")

def _reshape(run_dir, chain, fan_out=None, discard_at=None, lines_at=None, short_at=None,
             forward_at=None, declared=None):
    """Rewrites a one-source `_ledger_run` or `_http_run` directory in place so the nodes
    `chain` sit between the listener `in` and `window`. `fan_out` is `{node: k}`: each event
    into the node leaves as k, its parent discarded; `lines_at` is `{offset: lines}` that node
    lost at a drain, `short_at` `{offset: n}` children it sent short with nothing counted, and
    `forward_at` `{offset: n}` parents it sent on beside their children instead of discarding,
    as `line_protocol` does with a parent that carries metrics.
    `discard_at` is `{(node, offset): n}` events another node discarded at a drain. `window`'s
    received and absorbed counts and each life's V follow what the chain sent, so every
    identity holds; `declared` replaces the source's `fan_out`, which defaults to the
    telemetry's."""
    fan_out, discard_at = fan_out or {}, discard_at or {}
    lines_at, short_at, forward_at = lines_at or {}, short_at or {}, forward_at or {}
    factor = 1
    for node in chain:
        factor *= fan_out.get(node, 1)
    out, delta, shift = [], {0: 0, 1: 0}, {}
    for line in (run_dir / "logs" / "logit.stdout").read_text().splitlines():
        record = json.loads(line)
        metric = record["metrics"][0]
        out.append(record)
        if (record["attributes"].get("component") != "in"
                or metric["name"] != "logit.component.events.sent"):
            continue
        stamp = record["timestamp"]
        offset = round(telemetry.parse_rfc3339(stamp) - T0)
        x = e = metric["value"]
        for node in chain:
            k = fan_out.get(node)
            received = x
            forwarded = forward_at.get(offset, 0) if k else 0
            discarded = received - forwarded if k else discard_at.get((node, offset), 0)
            lost = lines_at.get(offset, 0) if k else 0
            x = ((k * received - lost - short_at.get(offset, 0) + forwarded) if k
                 else received - discarded)
            for name, value, attrs in (
                    ("logit.component.events.received", received, None),
                    ("logit.component.events.sent", x, None),
                    ("logit.component.events.discarded", discarded, None),
                    ("logit.transform.lines.dropped", lost, {"reason": "malformed"})):
                if value or name.endswith(("received", "sent")):
                    out.append(json.loads(_line(stamp, name, "sum", value, component=node,
                                                attrs=attrs)))
        shift[stamp] = x - e
        delta[0 if offset < STOP[0] else 1] += x - factor * e
    for record in out:
        if (record["attributes"].get("component") == "window"
                and record["metrics"][0]["name"] in ("logit.component.events.received",
                                                     "logit.transform.metrics.absorbed")):
            record["metrics"][0]["value"] += shift.get(record["timestamp"], 0)
    (run_dir / "logs" / "logit.stdout").write_text(
        "\n".join(json.dumps(record) for record in out) + "\n")
    series = vm.parse_export((run_dir / "vm-export" / "sink.jsonl").read_text())
    for one in series:
        one["values"] = [value * factor for value in one["values"]]
    # Each life's total is its last sample per series, so x_1's carries the life's change.
    series[1]["values"][1] += delta[0]
    series[1]["values"][3] += delta[1]
    _write_export(run_dir, "sink", series)
    resolved = json.loads((run_dir / "scenario.resolved.json").read_text())
    source = dict(resolved["ledger"]["source"][0], sut_transforms=list(chain))
    declared = fan_out if declared is None else declared
    if declared:
        source["fan_out"] = dict(declared)
    resolved["ledger"]["source"] = [source]
    (run_dir / "scenario.resolved.json").write_text(json.dumps(resolved))
    return run_dir


def _fan_out_ledger():
    """`ledger.edge`'s fan-out term on a source's `sut_transforms`, and every row after it
    reading the listener's symbols carried through the chain (W′): the residual, R, a killed
    life's band, each life's egress, the summary's terms, and the end-to-end bound on an HTTP
    wire, with a discarding node beside the fan-out."""
    with tempfile.TemporaryDirectory() as tmp:
        def results(name, http=False, **kwargs):
            base = (_http_run(tmp, name, kind="otlp_in") if http
                    else _ledger_run(tmp, name))
            return {r.id: r for r in checks.run_all(_reshape(base, **kwargs))}

        rows = ("ledger.intake", "ledger.edge", "ledger.aggregate", "ledger.egress",
                "ledger.summary")
        fanned = results("fan-out", chain=["points", "count"], fan_out={"points": 4})
        for row in rows:
            expect(fanned[row].status == checks.PASS,
                   f"{row} PASSes a balanced fan-out chain, got {fanned[row]}")
        edge = fanned["ledger.edge"].detail
        expect("children[points] = T_out 136,000 − (T_in 34,000 − T_disc 34,000) = 136,000 == "
               "4 x T_in − T_lines[points] 136,000 (points emits 4 per event)" in edge
               and "T_in[count] 136,000 − T_disc[count] 0 = 136,000 == T_out[count] 136,000"
               in edge,
               f"ledger.edge judges the fan-out node's children, got {edge}")
        intake = fanned["ledger.intake"].detail
        expect("life 0: W′ − Ab 120 within [0, R]" in intake
               and "R 828,000 = (10,000 + 132 x 1,000) x 4 + 65 x 1,000 x 4" in intake,
               f"the residual and R are counted in W′, got {intake}")
        expect("W′ − V 0 = residual 120" in fanned["ledger.egress"].detail,
               f"a stopped life's egress gap reads W′, got {fanned['ledger.egress']}")
        expect("ledger.edge's chain terms 0" in fanned["ledger.summary"].detail,
               f"the summary sums the chain's terms, got {fanned['ledger.summary']}")

        # A killed life's band takes its ingest rate in W′: 4 x the last drain's 1,050 records
        # over 5 s.
        killed = _reshape(_ledger_run(tmp, "fan-out-killed"), chain=["points"],
                          fan_out={"points": 4})
        timeline = (killed / "timeline.jsonl").read_text().replace('"action": "stop"',
                                                                  '"action": "kill"')
        (killed / "timeline.jsonl").write_text(timeline)
        egress = {r.id: r for r in checks.run_all(killed)}["ledger.egress"].detail
        expect("life 0 (killed)" in egress and "ingest 840/s" in egress,
               f"a killed life's band reads the fan-out's rate, got {egress}")

        lost = results("fan-out-lines", chain=["points"], fan_out={"points": 4},
                       lines_at={200: 3})
        for row in rows:
            expect(lost[row].status == checks.PASS,
                   f"{row} PASSes children short by the lines the node counted dropped, got "
                   f"{lost[row]}")
        # Lines dropped and parents forwarded in the stopped life, whose residual and egress gap
        # read W′: each term of `Ledger.through`'s fan-out arithmetic.
        for name, kwargs in (("fan-out-lines-stopped", {"lines_at": {60: 3}}),
                             ("fan-out-forward-stopped", {"forward_at": {60: 5}})):
            stopped = results(name, chain=["points"], fan_out={"points": 4}, **kwargs)
            for row in rows:
                expect(stopped[row].status == checks.PASS,
                       f"{row} PASSes {name}, got {stopped[row]}")
        short = results("fan-out-short", chain=["points"], fan_out={"points": 4},
                        short_at={200: 3})
        expect(short["ledger.edge"].status == checks.FAIL
               and "off by -3 (points emits 4 per event)" in short["ledger.edge"].detail,
               f"children short with nothing counted FAIL the node, got {short['ledger.edge']}")
        expect(short["ledger.summary"].status == checks.FAIL,
               f"the summary FAILs the same shortfall, got {short['ledger.summary']}")
        wrong = results("fan-out-wrong-k", chain=["points"], fan_out={"points": 4},
                        declared={"points": 3})
        expect(wrong["ledger.edge"].status == checks.FAIL
               and "3 x T_in − T_lines[points]" in wrong["ledger.edge"].detail,
               f"a fan-out other than the declared k FAILs, got {wrong['ledger.edge']}")

        # A node after the fan-out discards children, in the final life and then in the stopped
        # one, whose residual and egress gap carry the discard through W′.
        for name, offset in (("fan-out-discard-final", 200), ("fan-out-discard-stopped", 60)):
            kept = results(name, chain=["points", "stale"], fan_out={"points": 4},
                           discard_at={("stale", offset): 40})
            for row in rows:
                expect(kept[row].status == checks.PASS,
                       f"{row} PASSes a fan-out with discards after it at +{offset}, got "
                       f"{kept[row]}")

        # On an HTTP wire V is bounded end to end by G less counted loss, carried through the
        # chain: 4 x G here.
        http = results("fan-out-http", http=True, chain=["points"], fan_out={"points": 4})
        for row in rows:
            expect(http[row].status == checks.PASS,
                   f"{row} PASSes a fan-out behind otlp_in, got {http[row]}")
        expect("carried through the chains' fan_out and discards to 216,000" in http[
            "ledger.summary"].detail,
            f"the end-to-end bound is carried through the chain, got {http['ledger.summary']}")


def _idle_writer():
    """`ledger.wire` on an HTTP source whose writer delivers nothing for a while, a drain with
    no `events.delivered` point, and then its backlog at once, with a judged window's edge in
    the idle stretch: G holds flat to the burst, so no loss shows at the edge. And the boundary
    rule at a boundary touching a judged run: an opposite-signed pair nets in full before the
    surplus run's allowance applies, up to the G that moved over the generator interval
    straddling the boundary and no further, and runs that don't touch never net."""
    idle = {_stamp(T0 + offset) for offset in range(160, 205, 5)}
    moved = {"in": 0, "out": 0}

    def hold(component, name):
        def edit(record):
            metric = record["metrics"][0]
            if (record["attributes"].get("component") != component
                    or metric["name"] != name):
                return record
            if record["timestamp"] in idle:
                moved[component] += metric["value"]
                return None
            if record["timestamp"] == _stamp(T0 + 205):
                metric["value"] += moved[component]
            return record
        return edit

    window = _http_fault("x1", "netem", "generator", 140, 150)
    with tempfile.TemporaryDirectory() as tmp:
        run_dir = _http_run(tmp, "idle-writer", kind="otlp_in", timeline_extra=window,
                            sut_edit=hold("in", "logit.component.events.sent"),
                            gen_edit=hold("out", "logit.component.events.delivered"))
        wire = {r.id: r for r in checks.run_all(run_dir)}["ledger.wire"]
        expect(wire.status == checks.PASS,
               f"a writer's idle drains hold G flat across the stall, got {wire}")

    # Judged windows that overlap, so their runs touch at +190 and +195, with W shifted between
    # drains: `shift` is `{offset: events}` added to the listener's `events.sent`.
    def wire_with(name, shift, gen_edit=None):
        def edit(record):
            metric = record["metrics"][0]
            if (record["attributes"].get("component") == "in"
                    and metric["name"] == "logit.component.events.sent"):
                metric["value"] += shift.get(
                    round(telemetry.parse_rfc3339(record["timestamp"]) - T0), 0)
            return record

        windows = (_http_fault("x1", "netem", "logit", 140, 150)
                   + _http_fault("x2", "netem", "generator", 190, 200))
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = _http_run(tmp, name, kind="otlp_in", timeline_extra=windows,
                                sut_edit=edit, gen_edit=gen_edit)
            return {r.id: r for r in checks.run_all(run_dir)}["ledger.wire"]

    # W 700 ahead of G at the boundary +195 and behind it at the next drain: an opposite-signed
    # pair inside the G that moved over the generator interval straddling it, 1,000, nets in
    # full.
    within = wire_with("boundary-within", {195: 700, 200: -700})
    expect(within.status == checks.PASS
           and any("700 netted across the boundary at +195s (G moved 1,000 over the generator "
                   "interval straddling it), net gap 0" in line for line in within.lines),
           f"an opposite-signed pair at a judged boundary nets inside the straddling G, got "
           f"{within}")
    # The drain-skew case: a pair of 900 whose surplus run has six transport-error attempts,
    # an allowance of 800. Netting only the surplus past the allowance would move 100 and leave
    # 800 of loss, past the pool's 500; the pair nets in full first.
    skew = wire_with("boundary-skew", {195: 900, 200: -900},
                     gen_edit=_at_offset("logit.output.requests", 195, 6, "out",
                                         attrs={"class": "network_error"}))
    expect(skew.status == checks.PASS
           and any("900 netted across the boundary at +195s" in line and "net gap 0" in line
                   and "surplus allowed 800 (6 transport-error attempt(s)" in line
                   for line in skew.lines),
           f"a pair nets in full before the surplus run's allowance applies, got {skew}")
    # The same pair beyond that G: only 1,000 nets, and the rest FAILs both ways.
    beyond = wire_with("boundary-beyond", {195: 1400, 200: -1400})
    expect(beyond.status == checks.FAIL
           and any("1,000 netted across the boundary at +195s" in line
                   for line in beyond.lines)
           and "W − X exceeds G by 400" in beyond.detail,
           f"a pair beyond the straddling G FAILs, got {beyond}")
    # A surplus of 60 between two judged runs with no opposite-signed neighbour, the run after
    # it gapping 0: each of its two edges allows a batch of interpolation error.
    edged = wire_with("boundary-edges", {195: 60, 250: -60})
    expect(edged.status == checks.PASS
           and any(line.startswith("[+190s, +195s] judged") and "gap -60" in line
                   and "2 edge(s)" in line for line in edged.lines),
           f"a judged run between two others has two edges' allowance, got {edged}")
    # A loss of 800 next to a surplus of 150 that its run's two edges would excuse: the
    # surplus nets, and the 650 of loss left stands past the pool's 500.
    excused = wire_with("boundary-excused", {195: 150, 200: -800})
    expect(excused.status == checks.FAIL
           and any("150 netted across the boundary at +195s" in line
                   and "net gap 650" in line for line in excused.lines)
           and excused.detail.startswith("650 lost outside reported HTTP windows"),
           f"a surplus nets only itself against a loss next to it, got {excused}")
    # A judged run's loss of 1,600 at the boundary +245 next to a steady run's surplus of 1,600:
    # the steady surplus nets the loss only up to the 1,000 of G that moved over the generator
    # interval straddling +245, so 600 of loss stands past the pool's 500.
    steady = wire_with("boundary-steady", {245: -1600, 250: 1600})
    expect(steady.status == checks.FAIL
           and any(line.startswith("[+245s, +300s] steady")
                   and "1,000 netted across the boundary at +245s" in line
                   for line in steady.lines)
           and steady.detail.startswith("600 lost outside reported HTTP windows"),
           f"a steady surplus nets a judged loss only up to the straddling G, got {steady}")
    # A loss in one judged window and a surplus in another that doesn't touch it: nothing
    # nets, so the loss and the surplus each FAIL.
    apart = wire_with("boundary-apart", {150: -800, 220: 800})
    expect(apart.status == checks.FAIL
           and not any("netted" in line for line in apart.lines)
           and "[+195s, +245s] judged (x2 netem on generator): W − X exceeds G by 800"
           in apart.lines[1]
           and apart.detail.startswith("800 lost outside reported HTTP windows"),
           f"a loss next to a surplus in another window FAILs, got {apart}")


def _boundary_skew():
    """`checks._boundary_nets` on the drain skew of `otlp-fan-out-conc`'s run
    `20261025T212954Z`: a restart's backlog flush put −2,154 before the boundary at +665 and
    +2,159 after it, with 4,950 of G moved over the generator interval straddling it. The pair
    nets in full, whatever the surplus run's resend allowance."""
    runs = [[629.0, 665.0, "judged", "pause, stop", 1196, 3350, 0],
            [665.0, 670.0, "judged", "stop", 4659, 2500, 0]]
    anchors = [(660.0, 100_000.0), (667.0, 104_950.0)]
    nets, netted = checks._boundary_nets(runs, anchors)
    expect([round(net) for net in nets] == [0, 5]
           and netted.get(0) == [(2154, 665.0, 4950.0)],
           f"the drain-skew pair nets 2,154 across +665, leaving 5 of loss, got {nets} {netted}")
    # The same pair beside a straddling G of 1,000 nets only that far.
    capped, _ = checks._boundary_nets(runs, [(660.0, 0.0), (667.0, 1000.0)])
    expect([round(net) for net in capped] == [-1154, 1159],
           f"the transfer stops at the straddling G, got {capped}")
    # A steady run beside a judged one nets; a reported or end run never does.
    for kind, want in (("steady", [0, 5]), ("fault", [-2154, 2159]), ("end", [-2154, 2159])):
        mixed = [runs[0], [*runs[1][:2], kind, *runs[1][3:]]]
        got, _ = checks._boundary_nets(mixed, anchors)
        expect([round(net) for net in got] == want,
               f"a judged run beside a {kind} run nets to {want}, got {got}")


def _fan_out_rules(root):
    """`load()`'s rules for a source's `fan_out`, and the shipped otlp-fan-out scenario."""
    path, raw = _base(root)
    sut = ("components:\n  statsd: { type: statsd_in, bind: 0.0.0.0:8125 }\n"
           "  points: { type: line_protocol, sources: [statsd] }\n"
           "  stale: { type: drop_stale, sources: [points], max_age: 30s }\n")

    def problems(overlap=False, **source):
        with tempfile.TemporaryDirectory() as tmp:
            changed = copy.deepcopy(raw)
            for key, name in changed["configs"].items():
                text = sut if key == "sut" else (path.parent / name).read_text()
                (Path(tmp) / name).write_text(text)
            changed["ledger"]["source"][0].update(sut_transforms=["points", "stale"], **source)
            if overlap:
                changed["overlap"] = {"holder": "holder", "instances": ["logit", "logit-b"],
                                      "label": "instance"}
            try:
                scenario.from_dict(changed, Path(tmp) / "scenario.toml")
                return []
            except scenario.ScenarioError as err:
                return err.problems

    good = problems(fan_out={"points": 4})
    expect(good == [], f"a fan_out on a line_protocol node loads, got {good}")
    for source, needle in (
            ({"fan_out": 4}, "`fan_out` must be a table"),
            ({"fan_out": {}}, "`fan_out` must be a table"),
            ({"fan_out": {"points": 1}}, "must be an integer of at least 2"),
            ({"fan_out": {"points": True}}, "must be an integer of at least 2"),
            ({"fan_out": {"points": "4"}}, "must be an integer of at least 2"),
            ({"fan_out": {"points": 2.5}}, "must be an integer of at least 2"),
            ({"fan_out": {"other": 4}}, "`fan_out` names 'other', which isn't in"),
            ({"fan_out": {"stale": 4}}, "`fan_out` names 'stale', a drop_stale; only a "
                                        "line_protocol")):
        found = problems(**source)
        expect(any(needle in problem for problem in found),
               f"{source} is refused with {needle!r}, got {found}")
    found = problems(overlap=True, fan_out={"points": 4})
    expect(any("[overlap] and a source's `fan_out` can't be combined" in problem
               for problem in found), f"fan_out under [overlap] is refused, got {found}")

    fan = scenario.load(root / "scenarios" / "otlp-fan-out" / "scenario.toml")
    expect(fan.sources[0].get("fan_out") == {"points": 4}
           and fan.sources[0].get("wire") == "http"
           and fan.sources[0].get("sut_transforms") == ["points", "count"],
           f"otlp-fan-out declares its fan-out over an http wire, got {fan.sources}")


# A `sut_delta` fixture: a generator `generate_in -> aggregate (cumulative) -> otlp_out` sending
# `DELTA_SERIES` points a 5 s drain, each window's `DELTA_PER_DRAIN` increments split evenly
# between them, into SUT `otlp_in -> cumulative_to_delta -> datadog_out`.
DELTA_LEDGER = {"source": [{"service": "generator", "generator_input": "load",
                            "generator_sink": "out", "sut_listener": "in", "wire": "http"}],
                "sut_delta": "delta", "wire_loss_outside_faults": 0.0, "split": "copy",
                "sink": [{"id": "sink"}]}
DELTA_SERIES = 2
DELTA_PER_DRAIN = 100
DELTA_SUT = ("components:\n  in: { type: otlp_in, bind: 0.0.0.0:4318, protocol: http }\n"
             "  delta: { type: cumulative_to_delta, sources: [in] }\n"
             "  sink: { type: datadog_out, sources: [delta] }\n")
DELTA_GENERATOR = ("components:\n  load: { type: generate_in }\n"
                   "  totals: { type: aggregate, sources: [load], interval: 5s,"
                   " temporality: cumulative }\n"
                   "  out: { type: otlp_out, sources: [totals] }\n")


def _delta_run(tmp, name, restart=False, evict_at=None, edit=None, timeline_extra=(),
               sut_extra=(), gen_extra=(), action="stop"):
    """A `sut_delta` run directory that balances in points and in value. The generator sends
    from +5 to +300; each SUT drain receives one window's points and sends their deltas, each
    stored by a `202` request a second before the drain. With `restart`, the SUT is stopped
    (or killed, by `action`) over `STOP`: the generator's points from it reach life 1 in its
    first drain, where the transform skips each series' first point, losing one window's
    increments, and the stand-in stores the rest by one request, their timestamps in life 0. `evict_at` makes the drain at
    that offset an idle eviction: each series evicted and its point skipped, a window's
    increments lost. `edit(requests)` returns the record's request lines, edited."""
    per_point = DELTA_PER_DRAIN / DELTA_SERIES
    gen = []
    for offset in range(5, 301, 5):
        stamp = _stamp(T0 + offset)
        for metric, value, component in (
                ("logit.component.events.sent", DELTA_PER_DRAIN, "load"),
                ("logit.component.batches.sent", 1, "load"),
                ("logit.component.events.received", DELTA_SERIES, "out"),
                ("logit.component.batches.received", 1, "out"),
                ("logit.component.events.delivered", DELTA_SERIES, "out")):
            gen.append(_line(stamp, metric, "sum", value, component=component))
        gen.append(_line(stamp, "logit.process.uptime", "gauge", float(offset)))
        gen.append(_line(stamp, "logit.component.buffer.batches", "gauge", 0, component="out"))
    sut, requests = [], []
    drains = ([(o, 0) for o in range(5, STOP[0], 5)] + [(o, 1) for o in range(135, 321, 5)]
              if restart else [(o, 0) for o in range(5, 321, 5)])
    for offset, life in drains:
        stamp = _stamp(T0 + offset)
        sut.append(_line(stamp, "logit.process.uptime", "gauge",
                         float(offset - (STOP[1] if life else -1))))
        windows = [offset] if offset <= 300 else []
        if restart and offset == 135:
            # The windows the stop held back, the first of each series skipped.
            windows = list(range(105, 136, 5))
        received = DELTA_SERIES * len(windows)
        skipped = DELTA_SERIES if (restart and offset == 135) or offset == evict_at else 0
        sent = received - skipped
        stored = windows[1:] if skipped else windows
        for metric, value, component, attrs in (
                ("logit.component.events.sent", received, "in", None),
                ("logit.component.events.received", received, "delta", None),
                ("logit.component.events.sent", sent, "delta", None),
                ("logit.component.events.discarded", skipped, "delta", None),
                ("logit.transform.metrics.skipped", skipped, "delta", {"reason": "initial"}),
                ("logit.component.events.received", sent, "sink", None),
                ("logit.component.batches.received", 1 if sent else 0, "sink", None),
                ("logit.component.batches.delivered", 1 if sent else 0, "sink", None)):
            if value:
                sut.append(_line(stamp, metric, "sum", value, component=component, attrs=attrs))
        if offset == evict_at:
            sut.append(_line(stamp, "logit.transform.series.evicted", "sum", DELTA_SERIES,
                             component="delta", attrs={"reason": "idle"}))
        sut.append(_line(stamp, "logit.component.buffer.batches", "gauge", 0, component="sink"))
        sut.append(_line(stamp, "logit.component.retrying", "gauge", 0, component="sink"))
        if stored:
            series = [(f"x_{index}", [(int(T0) + window - 1, per_point) for window in stored])
                      for index in range(DELTA_SERIES)]
            requests.append(_datadog_request(len(requests) + 1, offset - 1, series))
    timeline = [_phase("start", 0, t0=T0)]
    if restart:
        timeline += [_action("apply", "c0s1", action, "logit", STOP[0],
                             affects_http_ingress=True),
                     _action("revert", "c0s1", action, "logit", STOP[1] - 1, STOP[1],
                             affects_http_ingress=True),
                     _ready("c0s1", "logit", STOP[1] + 1)]
    timeline += [_phase("end_begin", 300), _phase("end_end", 330)] + list(timeline_extra)
    run_dir = _run_dir(tmp, name, timeline, sut + list(sut_extra), ledger=DELTA_LEDGER,
                       target={"kind": "standin", "name": "standin", "env": [],
                               "shape": standin.DATADOG},
                       services={"logit": "sut", "generator": "generator", "standin": "standin"},
                       configs={"sut": "sut.yaml", "generator": "generator.yaml"})
    (run_dir / "configs").mkdir()
    (run_dir / "configs" / "sut.yaml").write_text(DELTA_SUT)
    (run_dir / "configs" / "generator.yaml").write_text(DELTA_GENERATOR)
    (run_dir / "logs" / "generator.stdout").write_text("\n".join(gen + list(gen_extra)) + "\n")
    stderr = [json.dumps({"timestamp": _stamp(T0 + offset), "level": "INFO",
                          "message": "drain complete", "target": "logit", "duration": "1ms"})
              for offset in ([STOP[0] + 0.6, 321] if restart else [321])]
    (run_dir / "logs" / "logit.stderr").write_text("\n".join(stderr) + "\n")
    requests = edit(requests) if edit else requests
    _write_standin(run_dir, requests, shape=standin.DATADOG)
    with open(run_dir / "logs" / "logit.stdout", "a") as out:
        out.write("\n".join(_standin_telemetry(requests)) + "\n")
    _write_export(run_dir, "sink", [])
    return run_dir


def _delta_ledger():
    """The `sut_delta` rows over `_delta_run`'s fixtures: the count rows in points, with the
    transform's skipped points counted and each stored point credited to the life its request
    reached the stand-in in; `ledger.value` exact with no reported window, a surplus and a loss
    FAILing it, a cued rejection's and a `series_errors` answer's deltas taken from the record,
    an SUT restart and an idle eviction reported inside their band, and a loss past the band
    FAILing; and an HTTP writer's own batch as the wire's unit."""
    with tempfile.TemporaryDirectory() as tmp:
        def rows(name, **kwargs):
            return {r.id: r for r in checks.run_all(_delta_run(tmp, name, **kwargs))}

        clean = rows("delta-clean")
        for row in ("ledger.value", "ledger.egress", "ledger.aggregate", "ledger.summary",
                    "ledger.edge", "ledger.standin"):
            expect(clean[row].status == checks.PASS,
                   f"a sut_delta run that balances PASSes {row}, got {clean[row]}")
        expect("I 6,000 (generate_in sent 6,000 − lost 0) − V 6,000 − U 0 = 0, exact"
               in clean["ledger.value"].detail,
               f"ledger.value judges the increments exact, got {clean['ledger.value'].detail}")
        led = checks.Ledger(checks.RunData(Path(tmp) / "delta-clean"))
        expect(led.interval == 5.0,
               f"under sut_delta the kill band's window is the generator's, got {led.interval}")
        expect(led.gen_stats["generator"]["batch"] == DELTA_SERIES,
               f"an HTTP writer's batch is its own, not its input's, got "
               f"{led.gen_stats['generator']['batch']}")

        def scaled(by):
            def edit(requests):
                request = requests[10]
                series = [("x_0", [(int(T0) + 54, DELTA_PER_DRAIN / DELTA_SERIES + by)]),
                          ("x_1", [(int(T0) + 54, DELTA_PER_DRAIN / DELTA_SERIES)])]
                requests[10] = _datadog_request(request["seq"], request["t"] - T0, series)
                return requests
            return edit

        short = rows("delta-short", edit=scaled(-10))
        expect(short["ledger.value"].status == checks.FAIL
               and "uncounted loss, with no reported window" in short["ledger.value"].detail
               and short["ledger.egress"].status == checks.PASS,
               f"a stored delta short of its increments FAILs ledger.value alone, got "
               f"{short['ledger.value']}")
        over = rows("delta-over", edit=scaled(10))
        expect(over["ledger.value"].status == checks.FAIL
               and "a surplus" in over["ledger.value"].detail,
               f"a stored delta past its increments is a surplus, got {over['ledger.value']}")

        # A 400 under a step classed `rejected`: its deltas are lost, and the record gives them.
        def rejected(cue):
            def edit(requests):
                requests[10].update(status=400, stored=False, cue=cue)
                return requests
            return edit

        drop = [_line(_stamp(T0 + 55), metric, "sum", value, component="sink",
                      attrs={"reason": "rejected"})
                for metric, value in (("logit.component.batches.dropped", 1),
                                      ("logit.component.events.dropped", DELTA_SERIES),
                                      ("logit.output.records.dropped", DELTA_SERIES))]
        cued = rows("delta-rejected", edit=rejected("c0s2"), sut_extra=drop,
                    timeline_extra=_respond_fault("c0s2", 50, 70, status=400, cls="rejected"))
        expect(cued["ledger.value"].status == checks.PASS
               and "U 100 = 0, exact; U 100 cued by c0s2" in cued["ledger.value"].detail,
               f"a cued rejection's deltas are U, taken from the record, got "
               f"{cued['ledger.value']}")
        uncued = rows("delta-uncued", edit=rejected(None), sut_extra=drop)
        expect(uncued["ledger.value"].status == checks.FAIL
               and "100 in points never stored outside a cue" in uncued["ledger.value"].detail,
               f"a delta never stored outside a cue is in the gap, got {uncued['ledger.value']}")

        def named(requests):
            requests[10]["series_errors"] = 1
            return requests

        errors = [_line(_stamp(T0 + 55), "logit.output.records.dropped", "sum", 1,
                        component="sink", attrs={"reason": "rejected"})]
        rows_named = rows("delta-series-errors", edit=named, sut_extra=errors,
                          timeline_extra=_respond_fault("c0s1", 50, 70, status=202,
                                                        series_errors=1))
        expect(rows_named["ledger.value"].status == checks.PASS
               and "U 50 = 0, exact" in rows_named["ledger.value"].detail,
               f"the series a series_errors answer named are U, got {rows_named['ledger.value']}")

        restarted = rows("delta-restart", restart=True)
        # The baseline is the wire's rate, one point per series per window, not the input's.
        expect(restarted["recovery"].status == checks.PASS,
               f"recovery's baseline behind a generator aggregate is its writer's events, got "
               f"{restarted['recovery']}")
        for row in ("ledger.egress", "ledger.aggregate", "ledger.summary", "ledger.edge"):
            expect(restarted[row].status == checks.PASS,
                   f"an SUT restart's skipped points and late-stamped deltas balance {row}, got "
                   f"{restarted[row]}")
        expect(restarted["ledger.value"].status == checks.PASS
               and "= 100, reported: within the 200 " in restarted["ledger.value"].detail
               and "life 1 start (2 skipped initial)" in " ".join(restarted["ledger.value"].lines),
               f"an SUT restart's lost window is reported inside its band, got "
               f"{restarted['ledger.value']}")
        extra = [_line(_stamp(T0 + 250), "logit.component.events.sent", "sum", 2000,
                       component="load")]
        past = rows("delta-restart-past", restart=True, gen_extra=extra)
        expect(past["ledger.value"].status == checks.FAIL
               and "so value was lost outside them" in past["ledger.value"].detail,
               f"a loss past the reported windows' band FAILs, got {past['ledger.value']}")
        # 300 increments lost at +250, far from the restart's window and under a band pooled
        # over its whole span (1,000), past what one skip of each series can lose.
        outside = [_line(_stamp(T0 + 250), "logit.component.events.sent", "sum", 300,
                         component="load")]
        hidden = rows("delta-restart-outside", restart=True, gen_extra=outside)
        expect(hidden["ledger.value"].status == checks.FAIL,
               f"a loss outside every window, under a pooled band, FAILs, got "
               f"{hidden['ledger.value']}")
        # The same loss beside a kill: the kill's window holds the points the killed life
        # lost, not its whole span.
        hidden = rows("delta-kill-outside", restart=True, action="kill", gen_extra=outside)
        expect(hidden["ledger.value"].status == checks.FAIL,
               f"a loss outside every window, beside a kill, FAILs, got {hidden['ledger.value']}")

        # Life 0's last two requests never stored: deltas a kill loses from the sink's queue.
        def lost_queue(requests):
            last = [i for i, r in enumerate(requests) if r["t"] < T0 + STOP[0]][-2:]
            return [r for i, r in enumerate(requests) if i not in last]

        stopped = rows("delta-stop-queue", restart=True, edit=lost_queue)
        expect(stopped["ledger.value"].status == checks.FAIL,
               f"a stop's window doesn't widen past its skips, got {stopped['ledger.value']}")
        killed = rows("delta-kill-queue", restart=True, action="kill", edit=lost_queue)
        expect(killed["ledger.value"].status == checks.PASS
               and "killed life 0's Ab − V 4 x 50" in " ".join(killed["ledger.value"].lines),
               f"a kill's window holds the points the killed life lost, got "
               f"{killed['ledger.value']}")
        dropped = [_line(_stamp(T0 + 110), "logit.component.batches.dropped", "sum", 1,
                         component="out", attrs={"reason": "overflow_oldest"})]
        overflowed = rows("delta-drop-queue", restart=True, edit=lost_queue, gen_extra=dropped)
        expect(overflowed["ledger.value"].status == checks.PASS
               and "widened, the generator dropped in it"
               in " ".join(overflowed["ledger.value"].lines),
               f"a generator drop widens its window, got {overflowed['ledger.value']}")

        evicted = rows("delta-evicted", evict_at=200)
        expect(evicted["ledger.value"].status == checks.PASS
               and "= 100, reported: within the 200 " in evicted["ledger.value"].detail
               and "idle eviction (2 skipped initial, 2 evicted)"
               in " ".join(evicted["ledger.value"].lines),
               f"an idle eviction's lost window is reported inside its band, got "
               f"{evicted['ledger.value']}")

        # A second point of one series in one second, as an upstream aggregate's shutdown flush
        # sends after its last window: two requests, two values, no resend.
        def same_second(requests):
            stamp = int(T0) + 99
            extra = _datadog_request(len(requests) + 1, 100.5, [("x_0", [(stamp, 5.0)])])
            return requests + [extra]

        pair = rows("delta-same-second", edit=same_second)
        expect(pair["ledger.standin"].status == checks.WARN
               and "Datadog keeps the later" in pair["ledger.standin"].detail
               and "a resend changed a sample" not in pair["ledger.standin"].detail,
               f"two points of one series in one second from two requests WARN as the known "
               f"gap, got {pair['ledger.standin']}")

        # Points skipped in a life the restart ended: its residual takes Δ_disc out.
        early = rows("delta-restart-evicted", restart=True, evict_at=50)
        expect(early["ledger.egress"].status == checks.PASS
               and "life 0: W − D − B − X − Δ_disc − V 0 = residual 0" in
               early["ledger.egress"].detail,
               f"a non-final life's skipped points leave no residual, got {early['ledger.egress']}")


def _delta_rules(root):
    """`[ledger] sut_delta`'s load-time rules, against the shipped delta-datadog scenario."""
    path = root / "scenarios" / "delta-datadog" / "scenario.toml"
    raw = tomllib.loads(path.read_text())
    loaded = scenario.load(path)
    expect(loaded.ledger.get("sut_delta") == "delta",
           f"delta-datadog loads with its sut_delta, got {loaded.ledger}")

    def mutate(change):
        changed = copy.deepcopy(raw)
        change(changed)
        return changed

    _refused(path, mutate(lambda r: r["ledger"].update(sut_aggregate="delta")),
             "ledger.sut_aggregate can't be combined with ledger.sut_delta",
             "sut_delta beside sut_aggregate")
    _refused(path, mutate(lambda r: r["ledger"].update(sut_delta="datadog")),
             "ledger.sut_delta names 'datadog', a datadog_out",
             "sut_delta naming a component of another kind")
    _refused(path, mutate(lambda r: r["target"].update(shape="influxdb")),
             "ledger.sut_delta needs a standin target with shape = \"datadog\"",
             "sut_delta under another stand-in shape")
    _refused(path, mutate(lambda r: r["ledger"]["source"][0].update(sut_transforms=["delta"])),
             "`sut_transforms` lists 'delta', the sut_delta node",
             "sut_delta in a source's chain")
    _refused(path, mutate(lambda r: r["ledger"].pop("sut_delta")),
             "ledger: missing `sut_aggregate`", "neither sut_delta nor sut_aggregate")
    for key, value in (("sut_script", "delta"),
                       ("hop", [{"from": "logit", "to": "collector", "sink": "hop",
                                 "listener": "hop"}]),
                       ("split", "route"), ("vm_every_window", True)):
        _refused(path, mutate(lambda r, key=key, value=value: r["ledger"].update({key: value})),
                 f"ledger.{key} can't be combined with ledger.sut_delta",
                 f"sut_delta beside {key}")
    _refused(path, mutate(lambda r: r.update(overlap={"instances": ["logit", "logit_b"]})),
             "[overlap] can't be combined with ledger.sut_delta", "sut_delta under [overlap]")
    _refused(path, mutate(lambda r: r["ledger"]["source"].append(
                 dict(r["ledger"]["source"][0]))),
             "ledger.sut_delta needs one [[ledger.source]]", "sut_delta with two sources")
    _refused(path, mutate(lambda r: r["ledger"]["source"][0].update(fan_out={"points": 4})),
             "`fan_out` can't be combined with ledger.sut_delta", "sut_delta with a fan_out")


# A scrape-wire fixture: a generator `generate_in -> aggregate (1 s, cumulative) ->
# prometheus_out` exposing `SCRAPE_SERIES` running totals, `SCRAPE_RATE` increments a second
# split evenly between them, scraped every 5 s by SUT `prometheus_in -> prometheus_out`.
SCRAPE_LEDGER = {"source": [{"service": "generator", "generator_input": "load",
                             "generator_sink": "out", "sut_listener": "scrape",
                             "wire": "scrape"}],
                 "wire_loss_outside_faults": 0.0, "split": "copy", "sink": [{"id": "sink"}]}
SCRAPE_SERIES = 2
SCRAPE_RATE = 100
SCRAPE_SUT = ("components:\n  scrape:\n    type: prometheus_in\n"
              "    scrape_targets: [\"http://generator:9464/metrics\"]\n    interval: 5s\n"
              "  sink: { type: prometheus_out, sources: [scrape], endpoint: http://vm/api/v1/write }\n")
SCRAPE_GENERATOR = ("components:\n  load: { type: generate_in }\n"
                    "  totals: { type: aggregate, sources: [load], interval: 1s,"
                    " temporality: cumulative }\n"
                    "  out: { type: prometheus_out, sources: [totals], bind: 0.0.0.0:9464 }\n")
# A generator kill's apply and revert offsets.
SCRAPE_KILL = (150, 180)


def _scrape_run(tmp, name, kill=False, value_at=None, sut_extra=(), gen_extra=(),
                failing=()):
    """A scrape run directory that balances: the generator generates from +0 to +300, each 5 s
    drain counting `SCRAPE_RATE` x 5 increments, and the SUT scrapes a second before each of its
    drains to +320, storing each series' running total at that time, which `value_at(t,
    start)` gives, by default every increment up to `t`. Each life ends with the increments
    after its last scrape lost, 100 at the end. With `kill`, the generator is killed over
    `SCRAPE_KILL`: its last drain interval's counts go unexported, and the scrapes in between
    fail. A scrape at an offset in `failing` times out, storing nothing."""
    value_at = value_at or (lambda t, start: SCRAPE_RATE / SCRAPE_SERIES * (t - start))
    lives = [(0, 0, SCRAPE_KILL[0]), (1, SCRAPE_KILL[1], 300)] if kill else [(0, 0, 300)]
    gen = []
    for life, start, end in lives:
        for offset in range(start + 5, end + (0 if kill and life == 0 else 1), 5):
            stamp = _stamp(T0 + offset)
            gen.append(_line(stamp, "logit.process.uptime", "gauge", float(offset - start)))
            gen.append(_line(stamp, "logit.component.events.sent", "sum", SCRAPE_RATE * 5,
                             component="load"))
    sut = []
    series = {index: {"metric": {"__name__": "soak_gen_total", "series": str(index)},
                      "values": [], "timestamps": []} for index in range(SCRAPE_SERIES)}
    for offset in range(5, 321, 5):
        stamp = _stamp(T0 + offset)
        scraped = offset - 1
        alive = next(((life, start) for life, start, end in lives if start < scraped < end),
                     None)
        ok = alive is not None and scraped <= 300 and scraped not in failing
        samples = SCRAPE_SERIES if ok else 0
        sut.append(_line(stamp, "logit.process.uptime", "gauge", float(offset + 1)))
        cls = "2xx" if ok else "timeout" if scraped in failing else "network_error"
        sent = samples + checks.SCRAPE_SYNTHETIC_SAMPLES
        for metric, value, component, attrs in (
                ("logit.input.scrapes", 1, "scrape", {"class": cls}),
                ("logit.input.samples", samples, "scrape", None),
                ("logit.component.events.sent", sent, "scrape", None),
                ("logit.component.batches.sent", 1, "scrape", None),
                ("logit.component.events.received", sent, "sink", None),
                ("logit.component.batches.received", 1, "sink", None),
                ("logit.component.events.delivered", sent, "sink", None),
                ("logit.component.batches.delivered", 1, "sink", None)):
            if value:
                sut.append(_line(stamp, metric, "sum", value, component=component, attrs=attrs))
        sut.append(_line(stamp, "logit.component.buffer.batches", "gauge", 0, component="sink"))
        sut.append(_line(stamp, "logit.component.retrying", "gauge", 0, component="sink"))
        if ok:
            for one in series.values():
                one["values"].append(value_at(scraped, alive[1]))
                one["timestamps"].append(int((T0 + scraped) * 1000))
    timeline = [_phase("start", 0, t0=T0)]
    if kill:
        timeline += [_action("apply", "c0s1", "kill", "generator", SCRAPE_KILL[0]),
                     _action("revert", "c0s1", "kill", "generator", SCRAPE_KILL[1] - 1,
                             SCRAPE_KILL[1]),
                     _ready("c0s1", "generator", SCRAPE_KILL[1] + 1)]
    timeline += [_phase("end_begin", 300), _phase("end_end", 330)]
    run_dir = _run_dir(tmp, name, timeline, sut + list(sut_extra), ledger=SCRAPE_LEDGER,
                       configs={"sut": "sut.yaml", "generator": "generator.yaml"})
    (run_dir / "configs").mkdir()
    (run_dir / "configs" / "sut.yaml").write_text(SCRAPE_SUT)
    (run_dir / "configs" / "generator.yaml").write_text(SCRAPE_GENERATOR)
    (run_dir / "logs" / "generator.stdout").write_text("\n".join(gen + list(gen_extra)) + "\n")
    (run_dir / "logs" / "logit.stderr").write_text(json.dumps(
        {"timestamp": _stamp(T0 + 321), "level": "INFO", "message": "drain complete",
         "target": "logit", "duration": "1ms"}) + "\n")
    _write_export(run_dir, "sink", list(series.values()))
    return run_dir


def _scrape_ledger():
    """The scrape wire's rows over `_scrape_run`'s fixtures: each generator life's G − V inside
    its end band, a killed life's band reaching below 0 and a stopped life's not, a loss or a
    surplus past the band FAILing, a failed scrape losing nothing, V split at the generator's
    lives, the synthetic samples in E as Y, the samples judged from the listener to the stored
    export, and recovery judged by a sample scraped."""
    with tempfile.TemporaryDirectory() as tmp:
        def rows(name, **kwargs):
            return {r.id: r for r in checks.run_all(_scrape_run(tmp, name, **kwargs))}

        clean = rows("scrape-clean")
        for row in ("ledger.wire", "ledger.intake", "ledger.edge", "ledger.egress",
                    "recovery", "identity.sink"):
            expect(clean[row].status == checks.PASS,
                   f"a scrape run that balances PASSes {row}, got {clean[row]}")
        for row in ("ledger.aggregate", "ledger.summary"):
            expect(clean[row].status == checks.SKIP,
                   f"a scrape run has no aggregate, so {row} SKIPs, got {clean[row]}")
        expect("generator life 0 (stopped): G 30,000 − V 29,900 = 100 within its end band "
               "[0, 1,100]" in clean["ledger.wire"].detail,
               f"a stopped life's lost tail is inside its end band, got {clean['ledger.wire']}")
        expect("W + Y − D 312 vs E + B 312" in clean["ledger.intake"].detail,
               f"the synthetic samples are Y beside W, got {clean['ledger.intake']}")
        expect("W − X 120 − V_n 120 = 0, exact" in clean["ledger.egress"].detail,
               f"every relayed sample is stored, got {clean['ledger.egress']}")

        # A stopped life's G can't fall short of V: its final drain exported every count.
        over = rows("scrape-surplus", value_at=lambda t, start: 51 * (t - start))
        expect(over["ledger.wire"].status == checks.FAIL
               and "outside its end band [0, 1,100], a surplus" in over["ledger.wire"].detail,
               f"stored totals past the increments FAIL as a surplus, got {over['ledger.wire']}")
        # Totals short of the increments by more than a life's tail.
        short = rows("scrape-short", value_at=lambda t, start: 40 * (t - start))
        expect(short["ledger.wire"].status == checks.FAIL
               and "increments lost past the life's last scrape" in short["ledger.wire"].detail,
               f"totals short of the increments FAIL, got {short['ledger.wire']}")

        # A scrape that times out stores nothing and loses nothing: the next one reads the
        # running total.
        timed_out = rows("scrape-timeout", failing=(99, 104, 109))
        expect(timed_out["ledger.wire"].status == checks.PASS
               and timed_out["ledger.egress"].status == checks.PASS,
               f"failed scrapes lose nothing from a running total, got "
               f"{timed_out['ledger.wire']} / {timed_out['ledger.egress']}")

        killed = rows("scrape-kill", kill=True)
        expect(killed["ledger.wire"].status == checks.PASS
               and "generator life 0 (killed): G 14,500 − V 14,900 = -400 within its end band "
               "[-500, 1,100]" in killed["ledger.wire"].detail
               and "generator life 1 (stopped): G 12,000 − V 11,900 = 100"
               in killed["ledger.wire"].detail,
               f"a killed life's unexported counts are inside its band, and V restarts with "
               f"the generator, got {killed['ledger.wire']}")
        expect(killed["recovery"].status == checks.PASS,
               f"a scrape wire recovers on a sample scraped, got {killed['recovery']}")
        # The same unexported counts in a life a stop ended: no band below 0.
        lost = [_line(_stamp(T0 + 150), "logit.component.events.dropped", "sum", 600,
                      component="load")]
        dropped = rows("scrape-dropped", gen_extra=lost)
        expect(dropped["ledger.wire"].status == checks.FAIL
               and "G 29,400 − V 29,900 = -500 outside" in dropped["ledger.wire"].detail,
               f"increments the generator counted lost leave G, got {dropped['ledger.wire']}")

        # A stored sample the listener never sent.
        extra = [_line(_stamp(T0 + 100), "logit.input.samples", "sum", -1, component="scrape")]
        surplus = rows("scrape-egress-surplus", sut_extra=extra)
        expect(surplus["ledger.egress"].status == checks.FAIL
               and "a surplus" in surplus["ledger.egress"].detail,
               f"a stored sample past W − X FAILs, got {surplus['ledger.egress']}")
        missing = [_line(_stamp(T0 + 100), "logit.input.samples", "sum", 1, component="scrape")]
        uncounted = rows("scrape-egress-loss", sut_extra=missing)
        expect(uncounted["ledger.egress"].status == checks.FAIL
               and "uncounted loss" in uncounted["ledger.egress"].detail,
               f"a sample neither stored nor counted dropped FAILs, got "
               f"{uncounted['ledger.egress']}")
        # The synthetic samples balance the listener's identity: without Y it's off.
        no_y = [_line(_stamp(T0 + 100), "logit.component.events.sent", "sum", 3,
                      component="scrape")]
        off = rows("scrape-intake-off", sut_extra=no_y)
        expect(off["ledger.intake"].status == checks.FAIL,
               f"an E past W + Y FAILs ledger.intake, got {off['ledger.intake']}")


def _scrape_rules(root):
    """The scrape wire's load-time rules, against the shipped prometheus-scrape scenario."""
    path = root / "scenarios" / "prometheus-scrape" / "scenario.toml"
    raw = tomllib.loads(path.read_text())
    loaded = scenario.load(path)
    expect(loaded.sources[0].get("wire") == "scrape" and "sut_aggregate" not in loaded.ledger,
           f"prometheus-scrape loads on the scrape wire with no aggregate, got {loaded.ledger}")
    try:
        scenario.check_services(loaded, scenario.base_roles(loaded))
        expect(True, "")
    except scenario.ScenarioError as err:
        expect(False, f"a kill on a scrape wire's generator is allowed, got {err.problems}")

    def mutate(change):
        changed = copy.deepcopy(raw)
        change(changed)
        return changed

    for key, value in (("sut_aggregate", "scrape"), ("sut_delta", "scrape"),
                       ("sut_script", "scrape"), ("split", "route"),
                       ("vm_every_window", True)):
        _refused(path, mutate(lambda r, key=key, value=value: r["ledger"].update({key: value})),
                 f"ledger.{key} can't be combined with a scrape wire",
                 f"a scrape wire beside {key}")
    _refused(path, mutate(lambda r: r["ledger"]["source"][0].update(
                 sut_transforms=["victoria_metrics"])),
             "`sut_transforms` can't be combined with a scrape wire", "a scrape wire's chain")
    _refused(path, mutate(lambda r: r["ledger"]["sink"].append(dict(r["ledger"]["sink"][0]))),
             "a scrape wire needs one [[ledger.sink]]", "a scrape wire with two sinks")
    _refused(path, mutate(lambda r: r["ledger"]["source"][0].update(generator_sink="load")),
             "a scrape wire's `generator_sink` must be a prometheus_out exposing on `bind:`, "
             "and 'load' is a generate_in", "a scrape wire's writer of another kind")
    _refused(path, mutate(lambda r: r["ledger"]["source"][0].update(wire="http")),
             "`wire` is http, but `sut_listener` 'scrape' is a prometheus_in on "
             "scrape_targets:, a scrape listener", "an http source on a scraping listener")
    # A kill stays SUT-only on every other wire.
    base, statsd = _base(root)
    statsd = copy.deepcopy(statsd)
    statsd["step"].append({"at": "1m", "action": "kill", "on": "generator", "for": "10s"})
    _refused(base, statsd, "kill is for logit only", "a kill on a udp wire's generator")


def _sampling_vectors(root):
    """sampling.py against xxHash's published seed-0 vectors and `logit_core::sampling`'s
    pinned ones, `keep`'s rate-0.5 edge, and the shipped sample-path scenario: its
    `vm_selector` names the shards its SUT's `sample` keeps and its `vm_absent_selector` the
    ones it discards, every shard the generator writes on one of the two."""
    for data, want in sampling.VECTORS:
        got = sampling.xxh64(data)
        expect(got == want, f"XXH64({data!r}) is {want:#018x}, got {got:#018x}")
    expect(sampling.keep(0x7FFF_FFFF_FFFF_FFFF, 0.5) and not sampling.keep(1 << 63, 0.5),
           "at rate 0.5 the threshold is 2^63")
    expect(sampling.keep(0, 1.0) and not sampling.keep(0, 0.0),
           "rate 1 keeps everything and rate 0 nothing")

    directory = root / "scenarios" / "sample-path"
    sut = (directory / "logit-sut.yaml").read_text()
    gen = (directory / "logit-generator.yaml").read_text()
    rate = float(re.search(r"^\s*rate:\s*([0-9.]+)", sut, re.M).group(1))
    shards = int(re.search(r"shard:\s*\"\{seq%([0-9]+)\}\"", gen).group(1))
    verdicts = sampling.verdicts(range(shards), rate)
    kept = {key for key, (_, keep) in verdicts.items() if keep}
    loaded = scenario.load(directory / "scenario.toml")
    sink = loaded.sinks[0]

    def named(selector):
        match = re.search(r'shard=~"([0-9|]+)"', selector or "")
        return set(match.group(1).split("|")) if match else None

    expect(named(sink.get("vm_selector")) == kept,
           f"sample-path's vm_selector names the kept shards {sorted(kept)}, got "
           f"{sink.get('vm_selector')}")
    expect(named(sink.get("vm_absent_selector")) == set(verdicts) - kept,
           f"sample-path's vm_absent_selector names the discarded shards "
           f"{sorted(set(verdicts) - kept)}, got {sink.get('vm_absent_selector')}")

    # A misspelled name or label in the absent selector matches nothing, and ledger.absent
    # would PASS on it, so the two selectors may differ only in their shard sets.
    def frame(selector):
        return re.sub(r'shard=~"[0-9|]+"', 'shard=~"*"', selector or "")

    expect(frame(sink.get("vm_absent_selector")) == frame(sink.get("vm_selector")),
           f"sample-path's vm_absent_selector is its vm_selector with only the shard set "
           f"swapped, got {sink.get('vm_absent_selector')} against {sink.get('vm_selector')}")
    expect(0 < len(kept) < shards, f"sample-path keeps some shards and discards some, got "
           f"{sorted(kept)} of {shards}")


def _absent_rules(root):
    """`load()`'s rules for a sink's `vm_absent_selector`."""
    path, raw = _base(root)

    def problems(target=None, **sink):
        changed = copy.deepcopy(raw)
        changed["ledger"]["sink"][0].update(sink)
        if target is not None:
            changed["target"] = target
            for key in ("backend", "vm_selector"):
                changed["ledger"]["sink"][0].pop(key, None)
        try:
            scenario.from_dict(changed, path)
            return []
        except scenario.ScenarioError as err:
            return err.problems

    good = problems(vm_absent_selector='{__name__="other_total"}')
    expect(good == [], f"a vm_absent_selector on a local sink loads, got {good}")
    selector = raw["ledger"]["sink"][0]["vm_selector"]
    for value in ("", 5, selector):
        found = problems(vm_absent_selector=value)
        expect(any("`vm_absent_selector` must be a selector other than `vm_selector`" in p
                   for p in found), f"vm_absent_selector {value!r} is refused, got {found}")
    found = problems(target={"kind": "external", "name": "datadog", "env": ["DD_API_KEY"]},
                     vm_absent_selector='{__name__="other_total"}')
    expect(any("`vm_absent_selector` names a backend query" in p for p in found),
           f"vm_absent_selector on an external target is refused, got {found}")


def _absent_run(tmp, name, series=(), phase="vm_absent_exported", error=None):
    """`_ledger_run`'s two-life run with `vm_absent_selector` on its sink: the absent export
    holds `series`, and the timeline records `phase` for it, none when None."""
    extra = []
    if phase == "vm_absent_exported":
        extra.append(_phase(phase, 329, sink="sink", series=len(series)))
    elif phase is not None:
        extra.append(_phase(phase, 329, sink="sink", error=error))
    run_dir = _ledger_run(tmp, name, timeline_extra=extra)
    resolved_path = run_dir / "scenario.resolved.json"
    resolved = json.loads(resolved_path.read_text())
    resolved["ledger"]["sink"][0]["vm_absent_selector"] = '{__name__="x_9_total"}'
    resolved_path.write_text(json.dumps(resolved))
    (run_dir / "vm-export" / f"sink{checks.ABSENT_SUFFIX}").write_text(
        "".join(json.dumps(one) + "\n" for one in series))
    return {r.id: r for r in checks.run_all(run_dir)}


def _absent_checks():
    """`ledger.absent`: an empty export PASSes, a series FAILs naming the lives its samples fall
    in, and an export that failed or never ran FAILs; a run with no selector SKIPs."""
    with tempfile.TemporaryDirectory() as tmp:
        plain = {r.id: r for r in checks.run_all(_ledger_run(tmp, "absent-none"))}
        expect(plain["ledger.absent"].status == checks.SKIP,
               f"a run with no vm_absent_selector SKIPs, got {plain['ledger.absent']}")
        clean = _absent_run(tmp, "absent-clean")["ledger.absent"]
        expect(clean.status == checks.PASS
               and clean.detail == 'sink: no series match {__name__="x_9_total"} in any of 2 '
               "SUT life/lives",
               f"an empty absent export PASSes, got {clean}")
        stored = {"metric": {"__name__": "x_9_total", "shard": "1"}, "values": [3, 4],
                  "timestamps": [int((T0 + 200) * 1000), int((T0 + 210) * 1000)]}
        held = _absent_run(tmp, "absent-held", series=[stored])["ledger.absent"]
        expect(held.status == checks.FAIL
               and "1 series match" in held.detail and "x_9_total{shard=1}" in held.detail
               and any("2 sample(s) in SUT life/lives 1" in line for line in held.lines),
               f"a stored series FAILs naming its life, got {held} {held.lines}")
        failed = _absent_run(tmp, "absent-failed", phase="vm_absent_export_failed",
                             error="timed out")["ledger.absent"]
        expect(failed.status == checks.FAIL and "the export failed: timed out" in failed.detail,
               f"a failed absent export FAILs, got {failed}")
        missing = _absent_run(tmp, "absent-missing", phase=None)["ledger.absent"]
        expect(missing.status == checks.FAIL
               and "no vm_absent_exported phase in the timeline" in missing.detail,
               f"an absent export that never ran FAILs, got {missing}")


def _merge_run(tmp, name, offset=200, n=7, merged=None, absorbed=0, declared=True):
    """`_ledger_run`'s two-life run with a `dedup_timestamps` node, `dedup`, between `in` and
    `window`: in the SUT drain at `offset` it merges `merged` records (default `n`) and
    discards the `n` events they emptied, counted, with V unchanged, since a merged increment
    reaches V in its survivor. `absorbed` records it superseded are counted beside them. With
    `declared` the source names the node in `merge`."""
    run_dir = _ledger_run(tmp, name)
    _discard(_with_chain(run_dir, ("dedup",)), ("dedup",), "dedup", offset, n)
    path = run_dir / "vm-export" / "sink.jsonl"
    series = vm.parse_export(path.read_text())
    same_life = (lambda s: s < STOP[1]) if offset < STOP[1] else (lambda s: s >= STOP[1])
    for one in series:
        if one["metric"]["__name__"] == "x_0_total":
            one["values"] = [value + n if offset <= ms / 1000 - T0 and same_life(ms / 1000 - T0)
                             else value for value, ms in zip(one["values"], one["timestamps"])]
    _write_export(run_dir, "sink", series)
    stdout = run_dir / "logs" / "logit.stdout"
    lines = [_line(_stamp(T0 + offset), "logit.transform.metrics.merged", "sum",
                   n if merged is None else merged, component="dedup")]
    if absorbed:
        lines.append(_line(_stamp(T0 + offset), "logit.transform.metrics.absorbed", "sum",
                           absorbed, component="dedup"))
    stdout.write_text(stdout.read_text() + "\n".join(lines) + "\n")
    if declared:
        resolved_path = run_dir / "scenario.resolved.json"
        resolved = json.loads(resolved_path.read_text())
        resolved["ledger"]["source"][0]["merge"] = ["dedup"]
        resolved_path.write_text(json.dumps(resolved))
    return {r.id: r for r in checks.run_all(run_dir)}


def _merge_ledger():
    """The merge term: a `merge` node's discards are its merged records, which `ledger.edge`
    holds them to, and every later row credits back as delivered (Ab + M), in the final life
    and in a stopped earlier one; a superseded record at the node FAILs the edge, and the same
    merges with no `merge` declared read as a surplus."""
    with tempfile.TemporaryDirectory() as tmp:
        final = _merge_run(tmp, "merge-final")
        for row in ("ledger.intake", "ledger.edge", "ledger.aggregate", "ledger.egress",
                    "ledger.summary", "drops"):
            expect(final[row].status == checks.PASS,
                   f"{row} PASSes merges credited in the final life, got {final[row]}")
        edge = final["ledger.edge"].detail
        expect("T_in[dedup] 34,000 − T_disc[dedup] 7 = 33,993 == T_out[dedup] 33,993" in edge
               and "T_disc[dedup] 7 == M[dedup] 7 (dedup discards only what it merged)" in edge
               and "T_ab[dedup] 0 == expected 0 (dedup supersedes no reading)" in edge,
               f"ledger.edge judges the merge node's discards and holds them to M, got {edge}")
        egress = final["ledger.egress"].detail
        expect("life 1 (final): Ab + M 33,993 + 7 = 34,000" not in egress
               and "life 1 (final): Ab + M 34,000 − V[sink] 34,000 = 0, ok" in egress,
               f"ledger.egress credits M back in the final life, got {egress}")
        summary = final["ledger.summary"].detail
        expect("Ab + M − V[sink] 0" in summary
               and "the transforms' ΣT_disc 7, of which M 7 merged and credited in Ab + M, "
               "counted 0" in summary,
               f"ledger.summary credits M back, got {summary}")

        stopped = _merge_run(tmp, "merge-stopped", offset=50)
        egress = stopped["ledger.egress"].detail
        expect(stopped["ledger.egress"].status == checks.PASS
               and "life 0: W − D − B − X − (ΣT_disc − M) − V 0 = residual 30" in egress,
               f"a stopped life's egress gap credits M back, got {egress}")
        intake = stopped["ledger.intake"].detail
        expect(stopped["ledger.intake"].status == checks.PASS
               and "life 0: W − D − B − X − (ΣT_disc − M) − (Ab + M) 30 within [0, R]" in intake,
               f"a stopped life's residual credits M back, got {intake}")

        undeclared = _merge_run(tmp, "merge-undeclared", declared=False)
        expect(undeclared["ledger.egress"].status == checks.FAIL
               and "Ab 33,993 − V[sink] 34,000 = -7, uncounted"
               in undeclared["ledger.egress"].detail,
               f"merges with no `merge` declared leave V above Ab, got "
               f"{undeclared['ledger.egress']}")

        short = _merge_run(tmp, "merge-short", merged=5)
        edge = short["ledger.edge"].detail
        expect(short["ledger.edge"].status == checks.FAIL
               and "T_disc[dedup] 7 != M[dedup] 5, off by 2" in edge,
               f"discards past the merged count FAIL the edge, got {edge}")
        superseded = _merge_run(tmp, "merge-absorbed", absorbed=2)
        edge = superseded["ledger.edge"].detail
        expect(superseded["ledger.edge"].status == checks.FAIL
               and "T_ab[dedup] 2 != expected 0, off by 2" in edge,
               f"a superseded reading at the merge node FAILs the edge, got {edge}")
        earlier = _merge_run(tmp, "merge-absorbed-earlier", offset=50, absorbed=2)
        edge = earlier["ledger.edge"].detail
        expect(earlier["ledger.edge"].status == checks.FAIL
               and "life 0: T_ab[dedup] 2 != expected 0, off by 2" in edge,
               f"a superseded reading in an earlier life FAILs the edge too, got {edge}")


def _merge_rules(root):
    """`load()`'s rules for a source's `merge`, and the shipped dedup-merge scenario."""
    path, raw = _base(root)
    sut = ("components:\n  statsd: { type: statsd_in, bind: 0.0.0.0:8125 }\n"
           "  dedup: { type: dedup_timestamps, sources: [statsd], precision: 1ms }\n"
           "  stale: { type: drop_stale, sources: [dedup], max_age: 30s }\n"
           "  points: { type: line_protocol, sources: [stale] }\n")

    def problems(chain=("dedup", "stale"), **source):
        with tempfile.TemporaryDirectory() as tmp:
            changed = copy.deepcopy(raw)
            for key, name in changed["configs"].items():
                text = sut if key == "sut" else (path.parent / name).read_text()
                (Path(tmp) / name).write_text(text)
            changed["ledger"]["source"][0].update(sut_transforms=list(chain), **source)
            try:
                scenario.from_dict(changed, Path(tmp) / "scenario.toml")
                return []
            except scenario.ScenarioError as err:
                return err.problems

    good = problems(merge=["dedup"])
    expect(good == [], f"a merge on a dedup_timestamps node loads, got {good}")
    for source, needle in (
            ({"merge": "dedup"}, "`merge` must list distinct `sut_transforms` nodes"),
            ({"merge": []}, "`merge` must list distinct `sut_transforms` nodes"),
            ({"merge": ["dedup", "dedup"]}, "`merge` must list distinct `sut_transforms` nodes"),
            ({"merge": ["other"]}, "`merge` names 'other', which isn't in"),
            ({"merge": ["stale"]}, "`merge` names 'stale', a drop_stale; only a "
                                   "dedup_timestamps merges records")):
        found = problems(**source)
        expect(any(needle in problem for problem in found),
               f"{source} is refused with {needle!r}, got {found}")
    found = problems(chain=("dedup", "points"), merge=["dedup"], fan_out={"points": 4})
    expect(any("`merge` and `fan_out` can't be combined" in problem for problem in found),
           f"merge beside fan_out is refused, got {found}")
    found = scenario._merge_problems({"split": "target"},
                                     {"sut_transforms": ["dedup"], "merge": ["dedup"]}, "here")
    expect(any("can't be combined with split = \"target\"" in problem for problem in found),
           f"merge under split = \"target\" is refused, got {found}")

    merged = scenario.load(root / "scenarios" / "dedup-merge" / "scenario.toml")
    expect(merged.sources[0].get("merge") == ["dedup"]
           and merged.sources[0].get("sut_transforms") == ["dedup"],
           f"dedup-merge declares its merge node, got {merged.sources}")


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
                     _log_capture, _signals, _deadlines, _stalled, _stall_abort,
                     _retry_stalled, _run_dir_claim, _hung_inspect, _inspect_redacted,
                     _ndjson, _stderr, _slope, _checks, _vm_resets, _ledger_checks,
                     _kill_watchdog, _kill_ledger, _expect_checks, _gauge_lives, _target_rules,
                     _sink_drain, _external_checks, _kafka_rules, _kafka_topic, _kafka_checks,
                     _otlp_walker, _otlp_topic, _otlp_checks,
                     _report, _expected_event_rules, _exit_driver, _ready_words, _exit_watchdog,
                     _script_ledger, _sink_ledger, _target_ledger, _roles, _sink_rules, _driver_roles,
                     _sources_ledger, _source_rules, _fanin_driver, _tcp_ledger, _tcp_rules,
                     _http_ledger, _http_rules, _file_ledger, _file_rules,
                     _two_sut_rules,
                     _two_sut_driver, _two_sut_checks, _per_service, _slope_edges,
                     _rss_settle_rules, _rss_settle_checks,
                     _expected_rules, _slope_reducer, _standin_codec, _standin_record_parts,
                     _standin_server, _respond_rules, _standin_count, _standin_idle_timeout,
                     _standin_driver,
                     _standin_checks, _class_signatures, _written_counts, _datadog_codec,
                     _datadog_counts, _datadog_server,
                     _datadog_rules, _datadog_checks, _influx_codec, _influx_points,
                     _influx_server, _influx_rules, _influx_checks, _overlap_rules,
                     _overlap_driver, _overlap_checks, _idp_rules, _refresh_checks,
                     _signal_rules, _signal_actions, _sighup_driver,
                     _reopen_checks, _tls_rules, _tls_actions, _tls_driver, _tls_checks,
                     _fan_out_ledger, _idle_writer, _boundary_skew, _fan_out_rules,
                     _sampling_vectors, _absent_rules, _absent_checks, _merge_ledger,
                     _merge_rules, _delta_ledger, _delta_rules, _scrape_ledger,
                     _scrape_rules, _shipped):
            try:
                takes_root = (_rules, _expect_rules, _expand, _random, _signals, _hung_inspect,
                              _stalled, _stall_abort,
                              _target_rules, _sink_drain, _kafka_rules, _otlp_walker,
                              _expected_event_rules,
                              _shipped, _roles, _sink_rules, _driver_roles, _source_rules,
                              _fanin_driver, _tcp_rules, _http_rules, _file_rules, _two_sut_rules,
                              _two_sut_driver,
                              _rss_settle_rules, _expected_rules, _standin_codec, _standin_server,
                              _respond_rules, _standin_count, _standin_idle_timeout,
                              _standin_driver, _written_counts, _datadog_codec,
                              _datadog_server,
                              _datadog_rules, _influx_codec, _influx_server, _influx_rules,
                              _overlap_rules, _overlap_driver, _idp_rules, _signal_rules,
                              _sighup_driver,
                              _tls_rules, _tls_driver, _fan_out_rules, _sampling_vectors,
                              _absent_rules, _merge_rules, _delta_rules, _scrape_rules)
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
