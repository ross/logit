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

A scenario's target is where the SUT's sinks deliver. The default is local: each
`[[ledger.sink]]` names the `backend` service the checks query for its V. A `[target]` table
with `kind = "external"` names a destination outside the stack, such as Datadog, which nothing
queries: the stack runs without the `local` profile's backends, a sink names no backend, and the
ledger ends at each sink's own telemetry (docs/plans/soak-harness.md's "External targets"). A
`[target]` table with `kind = "kafka"` runs a local Kafka broker, the `kafka` service, in place of
the `local` profile's backends: its one sink names no backend, faults on `kafka` are allowed for
that target only, and the ledger's V is the topic it names, read back at the end
(docs/plans/soak-harness.md's "Kafka targets"). A `[target]` table with `kind = "standin"` runs
the scripted stand-in destination, the `standin` service, in the same place: its one sink names
no backend, `respond` steps tell it what to answer, and the ledger's V is what its record says it
stored (docs/plans/soak-harness.md's "Stand-in targets").

Services are known by role, from each compose service's `soak.role` label: `sut` (a `logit`
under test), `generator`, `backend`, `broker` (a Kafka target's broker), `standin` (a
stand-in target's destination), or `netns` (a rolling overlap's namespace holder). A scenario may
add
services with a `compose` override file, a second generator or a second SUT included, so the
role map comes from the merged compose config,
which only the driver can read (`docker compose config`). `load()` checks what needs no service
list; `service_problems()` checks the rest against a role map: the driver's in `prepare()`
before the stack starts, and `base_roles()` in `list` and the self-test.
"""

import json
import os
import random
import re
import shutil
import tempfile
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

from . import standin as standin_mod

# The compose label naming a service's role, and the roles. A `sut` and a `generator` run
# `logit`; a `backend` stores what a sink delivers and answers the checks' queries; a `broker`
# is a Kafka target's broker, which the driver reads the topic back from; a `standin` is a stand-in
# target's scripted destination, which answers as `respond` steps cue it and records each request.
ROLE_LABEL = "soak.role"
SUT_ROLE, GENERATOR_ROLE, BACKEND_ROLE, BROKER_ROLE = "sut", "generator", "backend", "broker"
STANDIN_ROLE = "standin"
# A rolling overlap's namespace holder (`[overlap] holder`): a container that runs nothing but
# owns the network namespace its `instances` join, so a stopped instance doesn't take the port's
# address with it.
NETNS_ROLE = "netns"
ROLES = (SUT_ROLE, GENERATOR_ROLE, BACKEND_ROLE, BROKER_ROLE, STANDIN_ROLE, NETNS_ROLE)
LOGIT_ROLES = (SUT_ROLE, GENERATOR_ROLE)
# The ingress SUT: the service every generator sends to, whose config `[configs]` names under
# `sut`. A scenario's override can add one more SUT service, named by its own `[configs]` key
# and by the `[[ledger.hop]]`'s `to`, after `logit` in the chain (`Scenario.suts`); the last SUT
# holds `sut_aggregate` and every `[[ledger.sink]]`. Generators are any services with the role,
# each a `[[ledger.source]]`: compose.yaml's `generator`, and any a scenario's override adds.
SUT_SERVICE = "logit"
# The role map compose.yaml's labels give with the `local` profile, for what can't run
# `docker compose config`: `list` and the self-test, which pins it to compose.yaml.
BASE_ROLES = {"logit": SUT_ROLE, "generator": GENERATOR_ROLE, "victoria-metrics": BACKEND_ROLE}
# What the `kafka` profile adds to the role map in place of the `local` profile's backends.
KAFKA_ROLES = {"kafka": BROKER_ROLE}
# What the `standin` profile adds in their place.
STANDIN_ROLES = {"standin": STANDIN_ROLE}

# Every action the scenario schema accepts. faults.py implements them; the names must match its
# ACTIONS.
ACTION_NAMES = ("netem", "pause", "stop", "kill", "restart", "partition", "mark", "exit",
                "respond", "signal", "rename")
# Actions allowed on a SUT service only, each with the reason. A `kill` exists to end a `logit`
# process with no shutdown; on the generator, the lines it sent after its last telemetry drain
# reach the SUT with no G behind them, and `ledger.wire` would net them against real loss. An
# `exit` names a self-exit the SUT's own config causes, which the generator's config never does.
SUT_ONLY_ACTIONS = {
    "kill": ("on the generator, the lines it sent after its last telemetry drain reach the SUT "
             "with no G behind them, so ledger.wire would net them against real loss"),
    "exit": "an expected exit is one the SUT's own config causes",
}
# Actions that name an expected event rather than cause one: their apply and revert make no
# Docker call (an `exit`'s revert starts the container the event ended), so a `[random]`
# schedule, which draws faults, can't hold them.
EXPECTED_EVENT_ACTIONS = ("mark", "exit")
# Actions that open no fault window: a `signal` sends a `logit` service a signal it handles
# while it runs, and a `rename` moves a file aside in the run's shared directory, so neither
# excuses anything a check judges (checks.py's `RunData.events`). A `signal` may start inside a
# `rename` on its own service, and neither may overlap any other step on its service.
NO_WINDOW_ACTIONS = ("signal", "rename")
# The signals a `signal` step may send: SIGHUP, which a `logit` process answers by reopening
# its file targets and by reloading its TLS material (docs/adr/signal-handling.md).
SIGNALS = ("HUP",)
# The directory a `rename` works in, inside the containers that mount `<run>/shared`.
SHARED_MOUNT = "/shared"
# The `/readyz` words a `mark`'s `ready` may name: every answer but `ok`
# (crates/logit-cli/src/admin.rs, `readyz_wire`).
READY_WORDS = ("starting", "stalled", "draining", "degraded")
# A `respond` step's keys. It sets the stand-in's answer for its span: a `status` (with an
# optional `body`) or `answer = "reset"`, and `apply`, whether the stand-in stores the write it
# answers that way (by default, when the status is 2xx). Under the `datadog` shape a 2xx can
# carry `series_errors`, the number of the request's series its body names as dropped.
RESPOND_KEYS = ("status", "body", "apply", "answer", "series_errors")
RESPOND_ANSWERS = ("reset",)
# The sink fault classes a step's `class` may name (docs/adr/sink-fault-classes.md), which the
# checks read the SUT's telemetry and self-log against. `class` is for a `respond`, and for a
# `stop` of a stand-in, whose connect failures are `clean`.
FAULT_CLASSES = ("clean", "ambiguous", "rejected", "refused")
CLASSED_ACTIONS = ("respond", "stop")

# Faults during which a container's network namespace is gone or unusable for a one-shot
# `docker run --network container:<id>`. An `exit` can end the process at any point in its
# window.
NAMESPACE_FAULTS = ("stop", "kill", "pause", "partition", "restart", "exit")

TOP_LEVEL_KEYS = {
    "name", "description", "duration", "max_duration", "warmup", "cooldown", "recovery_bound",
    "cycle", "configs", "ledger", "thresholds", "step", "expect", "random", "target", "compose",
    "expected", "overlap",
}
# The local target, the default; the kind with no backend query; and the local Kafka broker,
# whose kind is also its service name.
LOCAL_TARGET = "victoria-metrics"
EXTERNAL_TARGET = "external"
KAFKA_TARGET = "kafka"
STANDIN_TARGET = "standin"
TARGET_KINDS = (LOCAL_TARGET, EXTERNAL_TARGET, KAFKA_TARGET, STANDIN_TARGET)
TARGET_KEYS = {"kind", "name", "env", "topic", "shape"}
# A stand-in target's shapes (standin/standin.py's `--shape`): the destination it plays.
STANDIN_SHAPES = standin_mod.SHAPES
STANDIN_DEFAULT_SHAPE = standin_mod.REMOTE_WRITE
DATADOG_SHAPE = standin_mod.DATADOG
# Kafka's legal topic names (`Topic.validate`): ASCII letters, digits, `.`, `_`, and `-`, up to
# 249 characters.
_TOPIC_NAME = re.compile(r"^[A-Za-z0-9._-]{1,249}$")
# The variables compose.yaml passes into the SUT's environment for an external target. A
# `[target] env` entry outside this list would reach no container, so `validate()` refuses it;
# the self-test checks that compose.yaml passes each one.
EXTERNAL_ENV_PASSED = ("DD_API_KEY",)
# The ledger keys that read an export: VictoriaMetrics', or a stand-in's record in its shape.
VM_LEDGER_KEYS = {"vm_every_window"}
_TARGET_NAME = re.compile(r"^[a-z0-9][a-z0-9_-]*$")
_ENV_NAME = re.compile(r"^[A-Z_][A-Z0-9_]*$")
PRIVATE_ENV_PREFIX = "soak-env-"
STEP_KEYS = {"at", "action", "on", "args", "for", "ready", "code", "key", "class", "signal",
             "path", "to", *RESPOND_KEYS}
RANDOM_KEYS = {"seed", "gap", "fault"}
RANDOM_FAULT_KEYS = {"weight", "action", "on", "args", "for"}
RANGE_KEYS = {"min", "max"}
# `[configs]` names the ingress SUT's config under `sut`, each generator's under its service
# name, and each further SUT service's under its service name.
SUT_CONFIG_KEY = "sut"
LEDGER_KEYS = {"sut_aggregate", "sink", "source", "wire_loss_outside_faults"}
OPTIONAL_LEDGER_KEYS = {"vm_every_window", "sut_script", "split", "hop"}
# A `[[ledger.hop]]`: the native hop between two SUT services, `from`'s `logit_out` component
# `sink` and `to`'s `logit_in` component `listener`. A scenario has at most one.
HOP_KEYS = {"from", "sink", "to", "listener"}
# The `[ledger]` keys a `[[ledger.source]]` table replaced, refused with a pointer to it.
SOURCE_LEDGER_KEYS = ("generator_input", "generator_sink", "sut_listener")
# A `[[ledger.source]]`: the generator service, its `generate_in` and sink, the SUT listener its
# lines reach, and, optionally, the wire they cross and the transforms between that listener
# and `aggregate`, each of which must forward every event it receives. `generator_input` and
# `generator_sink` may both list components, as many of each, whose counts sum (`source_ids()`),
# each input feeding the sink at its position (`source_pairs()`): a generator that sends several
# UDP flows has one input and one sink per flow.
SOURCE_KEYS = {"service", "generator_input", "generator_sink", "sut_listener"}
LISTED_SOURCE_KEYS = ("generator_input", "generator_sink")
OPTIONAL_SOURCE_KEYS = {"sut_transforms", "wire"}
# The wires a source's lines can cross into the SUT, each with its own ledger symbols
# (checks.py's `WIRES`): datagrams, a TCP stream of LF-terminated lines, or a file the SUT tails.
WIRES = ("udp", "tcp", "file")
DEFAULT_WIRE = "udp"
FILE_WIRE = "file"
# A file wire's writer: the only generator sink whose G is `events.delivered` (checks.py's
# `WIRES`), since it counts no `output.messages`.
FILE_WRITER_KIND = "file_out"
# Each listener kind the ledger can follow: the wire it takes with no `transport:`, and the
# wire each `transport:` value it accepts gives. A kind or transport missing here has no wire.
LISTENER_WIRES = {
    "statsd_in": ("udp", {"udp": "udp", "tcp": "tcp"}),
    "syslog_in": ("udp", {"udp": "udp", "tcp": "tcp"}),
    "lines_in": ("tcp", {"udp": "udp", "tcp": "tcp"}),
    "graphite_in": ("tcp", {"udp": "udp", "tcp": "tcp"}),
    "collectd_in": ("udp", {}),
    "tail_in": ("file", {}),
    "docker_in": ("file", {}),
}
# `[overlap]`: two or more SUT services, `instances`, each running the whole pipeline on one
# shared UDP port under `reuse_port`, in the network namespace of the `holder` service, each
# stamping its own name as the attribute `label` on every event ahead of `aggregate`, so V splits
# by instance (docs/plans/soak-harness.md's "Rolling overlap (W12b)").
OVERLAP_KEYS = {"holder", "instances", "label"}
# A Prometheus label name, which the label becomes on the backend.
_LABEL_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
# The actions allowed on the holder, and refused on an instance: an instance shares the holder's
# namespace, so a netem or a partition of it is one of the holder, and a lifecycle fault of the
# holder would take every instance's network away at once.
HOLDER_ACTIONS = ("netem", "partition")
# The actions that hold an instance's port or process, which may not overlap across instances:
# with every instance held at once nothing reads the port.
INSTANCE_HOLD_ACTIONS = ("pause", "stop", "kill", "restart", "exit")
# `docker stop -t` for a step's stop or restart (faults.py), which an overlap instance's
# `shutdown.delay` plus its drain must fit inside, or Docker kills it mid-drain.
STOP_TIMEOUT_S = 30
# The drain after `shutdown.delay`: the listeners' default 5 s `shutdown_grace` and a dedicated
# `internal` pipeline's 5 s second stage (docs/deploying.md's "Signal and restart behavior").
OVERLAP_DRAIN_S = 10
# A generator's service name, which also names its config's compose variable
# (`config_env()`), so it is a slug.
_SERVICE_NAME = re.compile(r"^[a-z0-9][a-z0-9_-]*$")
# How the SUT's sinks share `aggregate`'s output: `copy`, each sink a full copy by fan-out, or
# `route`, each sink fed by one `target` a `route` fills, so the sinks' V add up to it.
SPLITS = ("copy", "route")
# A `[[ledger.sink]]`: the sink's component id, the backend service it delivers to and the
# selector V is read with (both local only), and under `split = "route"` the `target` feeding it.
SINK_KEYS = {"id", "backend", "vm_selector", "target"}
_COMPONENT_ID = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.-]*$")
THRESHOLD_KEYS = {"progress_window", "rss_growth_mib_per_hour", "fd_growth"}
EXPECT_KEYS = {"name", "service", "metric", "component", "attrs", "step", "window", "reduce",
               "min", "max"}
# `run` is the only window with no step: the whole run after warmup.
EXPECT_WINDOWS = ("during", "after", "through", "run")
RUN_WINDOW = "run"
# Each reducer reads one metric kind: a counter (`sum`, a delta per drain) or a gauge.
EXPECT_REDUCERS = {"delta": "sum", "min_delta": "sum", "max": "gauge", "min": "gauge",
                   "last": "gauge", "slope": "gauge"}
# The rows an `[expected]` table may set a verdict on: the rows that judge each `logit` service
# apart, so a verdict names a service. A ledger row's FAIL is loss, never expected.
EXPECTED_ROWS = ("rss_slope", "fd_slope")
VERDICTS = ("PASS", "WARN", "FAIL")
# Kinds of the metrics the shipped expectations name, so `validate()` refuses a reducer of the
# wrong kind before a run. docs/design/internal-telemetry.md is the canonical list; a metric
# missing here is checked against the kind its points carry when the run is scored.
METRIC_KINDS = {
    "logit.input.datagrams": "sum",
    "logit.input.kernel.dropped": "sum",
    "logit.input.frames": "sum",
    "logit.input.frames.dropped": "sum",
    "logit.input.connections.closed": "sum",
    "logit.input.lines": "sum",
    "logit.input.files.rotated": "sum",
    "logit.input.files.resume_rejected": "sum",
    "logit.input.checkpoint.errors": "sum",
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
    "logit.output.requests": "sum",
    "logit.output.records": "sum",
    "logit.output.records.dropped": "sum",
    "logit.output.file.reopens": "sum",
    "logit.component.diagnostics": "sum",
    "logit.component.errors": "sum",
    "logit.component.events.dropped": "sum",
    "logit.component.events.discarded": "sum",
    "logit.component.flush.events": "sum",
    "logit.script.events.emitted": "sum",
    "logit.script.vm.memory": "gauge",
    "logit.process.interner.strings": "gauge",
    "logit.process.memory.resident.bytes": "gauge",
    "logit.transform.series.evicted": "sum",
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
    # A `mark`'s `ready` word, an `exit`'s `code` and `key`, a `respond`'s keys, and the fault
    # class a step expects; None elsewhere.
    ready: object = None
    code: object = None
    key: object = None
    status: object = None
    body: object = None
    apply: object = None
    answer: object = None
    series_errors: object = None
    fault_class: object = None
    # A `signal`'s signal, and a `rename`'s file and new name in the shared directory.
    signal: object = None
    path: object = None
    to: object = None


# The `Step` fields `expected()` writes into its JSON and its timeline records, each under its
# scenario key.
STEP_FIELDS = (("ready", "ready"), ("code", "code"), ("key", "key"), ("status", "status"),
               ("body", "body"), ("apply", "apply"), ("answer", "answer"),
               ("series_errors", "series_errors"), ("fault_class", "class"),
               ("signal", "signal"), ("path", "path"), ("to", "to"))


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
    ready: object = None
    code: object = None
    key: object = None
    status: object = None
    body: object = None
    apply: object = None
    answer: object = None
    series_errors: object = None
    fault_class: object = None
    signal: object = None
    path: object = None
    to: object = None

    def expected(self):
        """The fields this step sets (`STEP_FIELDS`), for its JSON and its timeline records."""
        return {name: getattr(self, attr) for attr, name in STEP_FIELDS
                if getattr(self, attr) is not None}

    def cue(self):
        """A `respond` step's cue for the stand-in's control port."""
        cue = {"step": self.id}
        for name in RESPOND_KEYS:
            value = getattr(self, name)
            if value is not None:
                cue[name] = value
        return cue

    def to_json(self):
        return {
            "id": self.id, "spec_index": self.spec_index, "cycle": self.cycle,
            "action": self.action, "on": self.on, "args": self.args,
            "start": self.start, "end": self.end, **self.expected(),
        }


@dataclass
class Expectation:
    """One `[[expect]]`: a bound on one metric, reduced over a window around a step.

    `step` is an expanded step id (`"c0s1"`) or a 1-based `[[step]]` index (an int), which
    names that step's occurrence in every cycle, and None under `window = "run"`. `attrs` holds
    every attribute filter, `component` included."""

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
    # `{kind, name, env}`, plus `topic` for a Kafka target; kind `victoria-metrics` (the
    # default), `external`, `kafka`, or `standin`.
    target: dict = field(default_factory=lambda: {"kind": LOCAL_TARGET, "name": LOCAL_TARGET,
                                                  "env": []})
    # The compose override file beside scenario.toml, or None.
    compose: object = None
    # `[expected]`: `{row id: {service: verdict}}`.
    expected: dict = field(default_factory=dict)
    # `max_duration`: the longest run the scenario's thresholds and configs hold, or None.
    max_duration: object = None
    # `[overlap]`: `{holder, instances, label}`, or None.
    overlap: object = None

    @property
    def external(self):
        return self.target.get("kind") == EXTERNAL_TARGET

    @property
    def kafka(self):
        return self.target.get("kind") == KAFKA_TARGET

    @property
    def standin(self):
        return self.target.get("kind") == STANDIN_TARGET

    @property
    def standin_shape(self):
        """A stand-in target's shape, or None for any other target."""
        return self.target.get("shape") if self.standin else None

    @property
    def has_vm(self):
        """Whether the stack runs the `local` profile's backends, which only the local target
        does."""
        return self.target.get("kind") == LOCAL_TARGET

    @property
    def sinks(self):
        """The `[[ledger.sink]]` tables, in order."""
        return list(self.ledger.get("sink") or [])

    @property
    def hops(self):
        """The `[[ledger.hop]]` tables, in order: none, or one."""
        return list(self.ledger.get("hop") or [])

    @property
    def sources(self):
        """The `[[ledger.source]]` tables, in order."""
        return list(self.ledger.get("source") or [])

    @property
    def file_sources(self):
        """The `[[ledger.source]]` tables whose `wire` is `file`, which the driver gives a
        shared directory and waits on at the end."""
        return [source for source in self.sources if source.get("wire") == FILE_WIRE]

    @property
    def uses_shared_dir(self):
        """Whether the run gets `<run>/shared`: for a file wire's writer and tailer, or for a
        `rename` step's file."""
        return bool(self.file_sources) or any(spec.action == "rename" for spec in self.steps)

    @property
    def generators(self):
        """The generator services, one per `[[ledger.source]]`, in order."""
        return [source.get("service") for source in self.sources]

    @property
    def extra_suts(self):
        """The `[configs]` keys that are neither `sut` nor a generator's: further SUT services,
        or overlap instances with a config of their own."""
        generators = set(self.generators)
        return [key for key in self.configs if key != SUT_CONFIG_KEY and key not in generators]

    @property
    def suts(self):
        """The SUT services: under `[overlap]` its `instances`, each a whole pipeline; else in
        chain order, `logit`, then each further SUT `[configs]` names, in its order."""
        if self.overlap is not None:
            return list(self.overlap["instances"])
        return [SUT_SERVICE] + self.extra_suts

    @property
    def sink_sut(self):
        """The SUT service holding `sut_aggregate` and every `[[ledger.sink]]`: the chain's last.
        Every overlap instance holds them."""
        return self.suts[-1]

    def sut_config_path(self, service):
        """The config of SUT `service`: `[configs] sut` for `logit` and for an overlap instance
        with no key of its own, else its own key."""
        return self.config_path(service if service in self.extra_suts else SUT_CONFIG_KEY)

    @property
    def split(self):
        return self.ledger.get("split", "copy")

    def compose_path(self):
        return None if self.compose is None else (self.directory / self.compose).resolve()

    @property
    def directory(self):
        return self.path.parent

    def config_path(self, key):
        """The config `[configs]` names under `key`: `sut`, or a generator's or a further SUT's
        service name."""
        return (self.directory / self.configs[key]).resolve()

    def seed_for(self, seed=None):
        """The seed a run uses: `seed` (from `--seed`) when given, else the `[random]` table's;
        None for a fixed schedule."""
        if self.random is None:
            return None
        return self.random.seed if seed is None else seed

    def to_json(self, duration=None, seed=None, services=None):
        """The resolved scenario a run records: its schedule expanded for `duration`, for a
        random schedule the seed it was drawn with, and `services`, the role map the run
        resolved, so `check` re-scores the run offline and `--seed` reproduces it."""
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
            "suts": self.suts,
            "overlap": None if self.overlap is None else {
                **self.overlap, "instances": list(self.overlap["instances"])},
            "compose": self.compose,
            "services": dict(services or {}),
            "target": target_json(self.target),
            "ledger": {**self.ledger, "split": self.split,
                       "sink": [dict(sink) for sink in self.sinks],
                       "source": [dict(source) for source in self.sources]},
            "thresholds": {
                "progress_window": self.thresholds["progress_window"],
                "rss_growth_mib_per_hour": self.thresholds["rss_growth_mib_per_hour"],
                "fd_growth": self.thresholds["fd_growth"],
            },
            "steps": [step.to_json() for step in expand(self, duration, seed)],
            "expect": [e.to_json() for e in self.expectations],
            "expected": {row: dict(services) for row, services in self.expected.items()},
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
    max_duration = dur(raw, "max_duration", "scenario") if "max_duration" in raw else None
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

    target = _parse_target(raw.get("target", {}), problems)
    # Only the local target has VictoriaMetrics to query, and a stand-in target's V has an
    # export's shape, so the keys naming one are refused for any other target with their own
    # message rather than ignored.
    no_vm = target["kind"] not in (LOCAL_TARGET, STANDIN_TARGET)
    configs = raw.get("configs", {})
    ledger = raw.get("ledger", {})
    thresholds = dict(raw.get("thresholds", {}))
    ledger_keys = LEDGER_KEYS - VM_LEDGER_KEYS if no_vm else LEDGER_KEYS
    ledger_optional = OPTIONAL_LEDGER_KEYS | (VM_LEDGER_KEYS if no_vm else set())
    if no_vm and isinstance(ledger, dict):
        for key in sorted(VM_LEDGER_KEYS & set(ledger)):
            problems.append(f"ledger.{key} names a VictoriaMetrics query, and the "
                            f"{target['kind']} target has no VictoriaMetrics")
    if isinstance(ledger, dict):
        for key in SOURCE_LEDGER_KEYS:
            if key in ledger:
                problems.append(f"ledger.{key} is a [[ledger.source]] key; each source names "
                                "its generator `service`, `generator_input`, `generator_sink`, "
                                "and `sut_listener`")
        ledger_optional = ledger_optional | (set(SOURCE_LEDGER_KEYS) & set(ledger))
    config_keys = {SUT_CONFIG_KEY}
    if isinstance(ledger, dict) and isinstance(ledger.get("source"), list):
        config_keys |= {source.get("service") for source in ledger["source"]
                        if isinstance(source, dict) and isinstance(source.get("service"), str)}
    # Every other `[configs]` key names a further SUT service; `service_problems()` checks that
    # the stack runs it with the role `sut`.
    extra_suts = ([key for key in configs if key not in config_keys]
                  if isinstance(configs, dict) else [])
    config_keys |= set(extra_suts)
    for table, keys, optional, where in (
        (configs, config_keys, set(), "configs"),
        (ledger, ledger_keys, ledger_optional, "ledger"),
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
    for key in sorted(config_keys & set(configs) if isinstance(configs, dict) else ()):
        if not isinstance(configs[key], str) or not (path.parent / configs[key]).is_file():
            problems.append(f"configs.{key}: {configs[key]} not found beside {path.name}")
    compose = raw.get("compose")
    if compose is not None:
        if not isinstance(compose, str) or not compose or "/" in compose:
            problems.append("`compose` must name an override file beside scenario.toml, such "
                            "as \"compose.override.yaml\"")
            compose = None
        elif not (path.parent / compose).is_file():
            problems.append(f"compose: {compose} not found beside {path.name}")
    overlap = _parse_overlap(raw["overlap"], problems) if "overlap" in raw else None
    if isinstance(ledger, dict):
        problems += _sink_problems(ledger, target["kind"])
        problems += _source_problems(ledger)
        sut_config = configs.get(SUT_CONFIG_KEY) if isinstance(configs, dict) else None
        if isinstance(sut_config, str) and (path.parent / sut_config).is_file():
            sut_text = (path.parent / sut_config).read_text()
            problems += _wire_problems(ledger, sut_text)
            problems += _route_problems(ledger, sut_text)
        problems += _writer_problems(ledger, configs, path.parent)
        problems += _extra_sut_problems(extra_suts, ledger)
        if overlap is None:
            problems += _hop_problems(ledger, extra_suts)
        elif isinstance(configs, dict):
            problems += _overlap_problems(overlap, ledger, configs, extra_suts, path.parent)

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
            ready=raw_step.get("ready"), code=raw_step.get("code"), key=raw_step.get("key"),
            status=raw_step.get("status"), body=raw_step.get("body"),
            apply=raw_step.get("apply"), answer=raw_step.get("answer"),
            series_errors=raw_step.get("series_errors"), fault_class=raw_step.get("class"),
            signal=raw_step.get("signal"), path=raw_step.get("path"), to=raw_step.get("to"),
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
        required = ["name", "service", "metric", "window", "reduce"]
        if raw_one.get("window") != RUN_WINDOW:
            required.insert(3, "step")
        for key in required:
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
            metric=raw_one.get("metric", ""), attrs=attrs,
            step=raw_one.get("step", None if raw_one.get("window") == RUN_WINDOW else ""),
            window=raw_one.get("window", ""), reduce=raw_one.get("reduce", ""),
            min=raw_one.get("min"), max=raw_one.get("max"),
        ))

    expected = _parse_expected(raw.get("expected", {}), problems)

    scenario = Scenario(
        path=path, name=name, description=description, duration=duration, warmup=warmup,
        cooldown=cooldown, recovery_bound=recovery_bound, cycle=cycle, configs=configs,
        ledger=ledger, thresholds=thresholds, steps=steps, expectations=expectations,
        random=random_spec, target=target, compose=compose, expected=expected,
        max_duration=max_duration, overlap=overlap,
    )
    if problems:
        raise ScenarioError(problems)
    validate(scenario)
    return scenario


def _parse_expected(table, problems):
    """`{row id: {service: verdict}}` from `[expected]`: each key one of `EXPECTED_ROWS`, each
    value a table of service names to one of `VERDICTS`. `service_problems()` checks that each
    service runs `logit`."""
    if not isinstance(table, dict):
        problems.append("`expected` must be a table, such as [expected] rss_slope = "
                        "{ logit = \"FAIL\" }")
        return {}
    expected = {}
    for row, services in table.items():
        where = f"expected.{row}"
        if row not in EXPECTED_ROWS:
            problems.append(f"{where}: only {', '.join(EXPECTED_ROWS)} take an expected verdict, "
                            "because only they judge each service apart")
            continue
        if not isinstance(services, dict) or not services:
            problems.append(f"{where}: must be a table of services to verdicts, such as "
                            "{ logit = \"FAIL\" }")
            continue
        for service, verdict in services.items():
            if verdict not in VERDICTS:
                problems.append(f"{where}.{service}: verdict must be one of "
                                f"{', '.join(VERDICTS)}, got {verdict!r}")
        expected[row] = dict(services)
    return expected


def target_json(target):
    """The resolved scenario's `target`: `{kind, name, env}`, plus `topic` for Kafka and
    `shape` for a stand-in."""
    out = {"kind": target["kind"], "name": target["name"], "env": list(target["env"])}
    for key in ("topic", "shape"):
        if key in target:
            out[key] = target[key]
    return out


def _sink_problems(ledger, kind):
    """What the `[[ledger.sink]]` tables and `[ledger] split` break: at least one sink, each a
    table of known keys with a distinct component `id`; for a local target a `backend` and a
    `vm_selector` on every sink, and for any other target neither; for a Kafka target one sink
    only, the `kafka_out` whose topic is V, and for a stand-in target one sink only; and under
    `split = "route"` at least two sinks, each with a distinct `target`, which `copy` refuses. Whether `backend` names a
    backend service needs the role map: `service_problems()`."""
    problems = []
    no_backend = kind != LOCAL_TARGET
    split = ledger.get("split", "copy")
    if split not in SPLITS:
        problems.append(f"ledger.split must be one of {', '.join(SPLITS)}, got {split!r}")
    sinks = ledger.get("sink")
    if "sink" not in ledger:
        return problems
    if not isinstance(sinks, list) or not sinks or not all(isinstance(t, dict) for t in sinks):
        return problems + ["ledger.sink must be one or more tables, written [[ledger.sink]]"]
    if kind == KAFKA_TARGET and len(sinks) != 1:
        problems.append("a kafka target has one [[ledger.sink]], the kafka_out whose topic the "
                        "ledger reads back")
    if kind == STANDIN_TARGET and len(sinks) != 1:
        problems.append("a standin target has one [[ledger.sink]], the sink whose writes the "
                        "stand-in records")
    ids, targets = [], []
    for index, sink in enumerate(sinks):
        where = f"ledger.sink {index + 1}"
        for key in sorted(set(sink) - SINK_KEYS):
            problems.append(f"{where}: unknown key `{key}`")
        sink_id = sink.get("id")
        if not isinstance(sink_id, str) or not _COMPONENT_ID.match(sink_id):
            problems.append(f"{where}: `id` must name the SUT sink's component id")
        elif sink_id in ids:
            problems.append(f"{where}: `id` {sink_id!r} repeats another sink's")
        else:
            ids.append(sink_id)
        if no_backend:
            for key in sorted({"backend", "vm_selector"} & set(sink)):
                problems.append(f"{where}: `{key}` names a backend query, and the {kind} "
                                "target has no backend")
        else:
            for key in ("backend", "vm_selector"):
                if not isinstance(sink.get(key), str) or not sink.get(key):
                    problems.append(f"{where}: missing `{key}`; a local sink names the backend "
                                    "service it delivers to and the selector its V is read with")
        target = sink.get("target")
        if split == "route":
            if not isinstance(target, str) or not _COMPONENT_ID.match(target):
                problems.append(f"{where}: under split = \"route\" a sink needs `target`, the "
                                "id of the target that feeds it")
            elif target in targets:
                problems.append(f"{where}: `target` {target!r} repeats another sink's")
            else:
                targets.append(target)
        elif target is not None:
            problems.append(f"{where}: `target` is for split = \"route\"")
    if split == "route" and len(sinks) < 2:
        problems.append("ledger.split = \"route\" needs two or more [[ledger.sink]] tables")
    return problems


def component_blocks(text, component):
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


def listener_wire(text, component):
    """`(wire, kind, transport)` for SUT listener `component` in config `text`: the wire its
    `type:` and `transport:` give (`LISTENER_WIRES`), or None for a kind or transport the ledger
    has no wire for; `(None, None, None)` when no block of `component` names a `type:`. Comments
    are ignored."""
    for block in component_blocks(text, component):
        body = "\n".join(re.sub(r"(^|\s)#.*$", "", line) for line in block.splitlines())
        kind = re.search(r"\btype\s*:\s*[\"']?([a-z_]+)", body)
        if not kind:
            continue
        found = re.search(r"\btransport\s*:\s*[\"']?([a-z_]+)", body)
        transport = found.group(1) if found else None
        default, by_transport = LISTENER_WIRES.get(kind.group(1), (None, {}))
        wire = default if transport is None else by_transport.get(transport)
        return wire, kind.group(1), transport
    return None, None, None


def _route_blocks(text, kind="route"):
    """`(id, body)` for each `type: <kind>` component in config `text`, comments removed: the
    component's own header line and every deeper-indented line after it."""
    lines = [re.sub(r"(^|\s)#.*$", "", line) for line in (text or "").splitlines()]
    for index, line in enumerate(lines):
        if not re.search(r"\btype\s*:\s*[\"']?" + re.escape(kind) + r"\b", line):
            continue
        indent = len(line) - len(line.lstrip())
        head = index
        if re.match(r"^\s*[\w.-]+\s*:\s*\{", line):
            pass
        else:
            while head > 0 and (not lines[head].strip()
                                or len(lines[head]) - len(lines[head].lstrip()) >= indent):
                head -= 1
        found = re.match(r"^\s*([\w.-]+)\s*:(.*)$", lines[head])
        if not found:
            continue
        head_indent = len(lines[head]) - len(lines[head].lstrip())
        block = [found.group(2)]
        for following in lines[head + 1:]:
            if following.strip() and len(following) - len(following.lstrip()) <= head_indent:
                break
            block.append(following)
        yield found.group(1), "\n".join(block)


def _routes_value(body):
    """The text of a route component's `routes:` value in `body`: a flow mapping to its closing
    brace, or the deeper-indented lines of a block mapping; empty when there is none."""
    found = re.search(r"\broutes\s*:[ \t]*(\{)?", body)
    if not found:
        return ""
    if found.group(1):
        depth, end = 0, found.end() - 1
        for position in range(found.end() - 1, len(body)):
            depth += {"{": 1, "}": -1}.get(body[position], 0)
            if depth == 0:
                end = position
                break
        else:
            end = len(body)
        return body[found.end() - 1:end + 1]
    start = body.rfind("\n", 0, found.start()) + 1
    indent = found.start() - start
    out = []
    for line in body[found.end():].split("\n")[1:]:
        if line.strip() and len(line) - len(line.lstrip()) <= indent:
            break
        out.append(line)
    return "\n".join(out)


def _route_problems(ledger, sut_text):
    """Under `split = "route"`, the sinks' V add up only if a router sends each event to one
    target. A `routes:` value that is a list can deliver one event to several
    sinks, so the SUT config's `route` components must use none. A `route_signals` can send
    parts of one event to several sinks under either `strip:` value, so it is refused outright."""
    if ledger.get("split", "copy") != "route":
        return []
    problems = []
    for name, _ in _route_blocks(sut_text, "route_signals"):
        problems.append(f"ledger.split = \"route\": route_signals {name!r} can deliver parts of "
                        "one event to several sinks, so their totals don't add up; use "
                        "split = \"copy\" or a route with one target per value")
    for name, body in _route_blocks(sut_text):
        listed = re.search(r"\[|^\s*-\s", _routes_value(body), re.M)
        if listed:
            problems.append(f"ledger.split = \"route\": route {name!r} uses a list-valued "
                            "`routes:` value, which can deliver one event to several sinks, so "
                            "their totals don't add up; use split = \"copy\" or one target "
                            "per value")
    return problems


def _wire_problems(ledger, sut_text):
    """What each `[[ledger.source]]`'s `wire` breaks against the SUT config `sut_text`: its
    `sut_listener` must be a component there, of a kind and transport `LISTENER_WIRES` gives a
    wire for, and that wire must be the source's own, `udp` when it names none."""
    sources = ledger.get("source")
    if not isinstance(sources, list):
        return []
    problems = []
    for index, source in enumerate(sources):
        listener = source.get("sut_listener") if isinstance(source, dict) else None
        if not isinstance(listener, str) or not _COMPONENT_ID.match(listener):
            continue
        where = f"ledger.source {index + 1}"
        declared = source.get("wire", DEFAULT_WIRE)
        wire, kind, transport = listener_wire(sut_text, listener)
        over = f"over {transport}" if transport else "with no transport:"
        if kind is None:
            problems.append(f"{where}: `sut_listener` {listener!r} isn't a component of the "
                            "SUT config")
        elif wire is None:
            problems.append(f"{where}: `sut_listener` {listener!r} is a {kind} {over}, which "
                            "the ledger has no wire for")
        elif declared in WIRES and wire != declared:
            problems.append(f"{where}: `wire` is {declared}, but `sut_listener` {listener!r} "
                            f"is a {kind} {over}, a {wire} listener")
    return problems


def source_ids(source, key):
    """A `[[ledger.source]]` key's component ids as a list: `generator_input` and
    `generator_sink` may name one or list several."""
    value = source.get(key)
    return list(value) if isinstance(value, list) else [value]


def source_pairs(source):
    """`[(generator_input, generator_sink)]`: each sink with the input that feeds it, by position
    when both keys list components, which `load()` holds to one length."""
    return list(zip(source_ids(source, "generator_input"), source_ids(source, "generator_sink")))


def _writer_problems(ledger, configs, directory):
    """What a source's `generator_sink` breaks against its generator's config: on a `file`
    wire it must be a `file_out`, and on any other wire it must not be, because the two read G
    from different counters."""
    sources = ledger.get("source")
    if not isinstance(sources, list) or not isinstance(configs, dict):
        return []
    problems = []
    for index, source in enumerate(sources):
        if not isinstance(source, dict):
            continue
        service = source.get("service")
        name = configs.get(service) if isinstance(service, str) else None
        sinks = source_ids(source, "generator_sink")
        if (not isinstance(name, str) or not all(isinstance(sink, str) for sink in sinks)
                or not (directory / name).is_file()):
            continue
        text = (directory / name).read_text()
        for sink in sinks:
            kind = None
            for block in component_blocks(text, sink):
                body = "\n".join(re.sub(r"(^|\s)#.*$", "", line) for line in block.splitlines())
                found = re.search(r"\btype\s*:\s*[\"']?([a-z_]+)", body)
                if found:
                    kind = found.group(1)
                    break
            where = f"ledger.source {index + 1}"
            wire = source.get("wire", DEFAULT_WIRE)
            if wire == FILE_WIRE and kind != FILE_WRITER_KIND:
                problems.append(f"{where}: a file wire's `generator_sink` must be a "
                                f"{FILE_WRITER_KIND}, and {sink!r} is "
                                + (f"a {kind}" if kind else "no component of its config"))
            elif wire != FILE_WIRE and kind == FILE_WRITER_KIND:
                problems.append(f"{where}: `generator_sink` {sink!r} is a {FILE_WRITER_KIND}, "
                                "which only a file wire reads")
    return problems


def _source_problems(ledger):
    """What the `[[ledger.source]]` tables break: at least one, each a table of known keys with
    every required key a component id, a distinct generator `service` that is a slug whose
    `config_env()` is neither `SUT_CONFIG_ENV` nor another source's, a distinct `sut_listener`,
    and an optional `sut_transforms` list of distinct component ids, none of them a listener or
    on another source's list; and no `sut_transforms` beside `[ledger] sut_script`, whose hop
    `ledger.script` judges from the listeners' summed E. Whether each `service` is a generator
    needs the role map: `service_problems()`."""
    if "source" not in ledger:
        return []
    sources = ledger.get("source")
    if (not isinstance(sources, list) or not sources
            or not all(isinstance(t, dict) for t in sources)):
        return ["ledger.source must be one or more tables, written [[ledger.source]]"]
    problems = []
    services, listeners = [], []
    for index, source in enumerate(sources):
        where = f"ledger.source {index + 1}"
        for key in sorted(set(source) - SOURCE_KEYS - OPTIONAL_SOURCE_KEYS):
            problems.append(f"{where}: unknown key `{key}`")
        for key in sorted(SOURCE_KEYS - set(source)):
            problems.append(f"{where}: missing `{key}`")
        service = source.get("service")
        if "service" in source:
            if not isinstance(service, str) or not _SERVICE_NAME.match(service):
                problems.append(f"{where}: `service` must name a generator service: lowercase "
                                "letters, digits, `_`, or `-`")
            elif service in services:
                problems.append(f"{where}: `service` {service!r} repeats another source's")
            elif config_env(service) in {SUT_CONFIG_ENV} | {
                    config_env(other) for other in services}:
                # `generator-lines` and `generator_lines` share one compose variable.
                problems.append(f"{where}: `service` {service!r} would mount its config "
                                f"through {config_env(service)}, which the SUT or another "
                                "source already uses")
            else:
                services.append(service)
        for key in ("generator_input", "generator_sink", "sut_listener"):
            value = source.get(key)
            if key in LISTED_SOURCE_KEYS and isinstance(value, list):
                if (not value or len(set(map(str, value))) != len(value)
                        or not all(isinstance(v, str) and _COMPONENT_ID.match(v)
                                   for v in value)):
                    problems.append(f"{where}: `{key}` must name a component id or list "
                                    "distinct ones")
            elif key in source and (not isinstance(value, str)
                                    or not _COMPONENT_ID.match(value)):
                problems.append(f"{where}: `{key}` must name a component id")
        if "wire" in source and source["wire"] not in WIRES:
            problems.append(f"{where}: `wire` must be one of {', '.join(WIRES)}")
        listed = [isinstance(source.get(key), list) for key in LISTED_SOURCE_KEYS]
        if any(listed) and not (all(listed) and len(source["generator_input"])
                                == len(source["generator_sink"])):
            problems.append(f"{where}: `generator_input` and `generator_sink` list the same "
                            "number of components, each input paired with the sink at its "
                            "position, or each names one")
        listener = source.get("sut_listener")
        if isinstance(listener, str) and _COMPONENT_ID.match(listener):
            if listener in listeners:
                problems.append(f"{where}: `sut_listener` {listener!r} repeats another "
                                "source's; each source reaches its own listener")
            else:
                listeners.append(listener)
    every = set(listeners)
    # Every transform named so far, across all chains: a node in two chains would be judged
    # as forwarding both sources' events.
    named = set()
    for index, source in enumerate(sources):
        where = f"ledger.source {index + 1}"
        if "sut_transforms" not in source:
            continue
        chain = source["sut_transforms"]
        if (not isinstance(chain, list) or not chain
                or not all(isinstance(c, str) and _COMPONENT_ID.match(c) for c in chain)):
            problems.append(f"{where}: `sut_transforms` must list the component ids between "
                            "the listener and aggregate, in order")
        elif len(set(chain)) != len(chain) or set(chain) & every:
            problems.append(f"{where}: `sut_transforms` repeats a component or names a "
                            "listener")
        elif set(chain) & named:
            problems.append(f"{where}: `sut_transforms` names "
                            f"{', '.join(sorted(set(chain) & named))}, which another source's "
                            "chain lists; each node is on one source's chain")
        else:
            named |= set(chain)
        if ledger.get("sut_script") is not None:
            problems.append(f"{where}: `sut_transforms` and [ledger] sut_script can't be "
                            "combined; the script's hop is judged from every listener's E")
    return problems


def _hop_problems(ledger, extra_suts):
    """What `[[ledger.hop]]` breaks: at most one table, of `HOP_KEYS` only, each a component id
    or a service; `from` is `SUT_SERVICE` and `to` the one further SUT service `[configs]`
    names; and no `sut_script` or `sut_transforms` beside it, whose rows judge a hop into
    `aggregate` inside one process. Two SUT services need a hop, since the ledger can't join
    them without one."""
    hops = ledger.get("hop")
    if hops is None:
        if extra_suts:
            return [f"configs: SUT services {SUT_SERVICE} and {', '.join(extra_suts)} need a "
                    "[[ledger.hop]] naming the logit_out and logit_in between them"]
        return []
    if (not isinstance(hops, list) or len(hops) != 1
            or not all(isinstance(hop, dict) for hop in hops)):
        return ["ledger.hop must be one table, written [[ledger.hop]]; a scenario has at most "
                "one hop"]
    hop = hops[0]
    problems = []
    for key in sorted(set(hop) - HOP_KEYS):
        problems.append(f"ledger.hop: unknown key `{key}`")
    for key in sorted(HOP_KEYS - set(hop)):
        problems.append(f"ledger.hop: missing `{key}`")
    for key in ("sink", "listener"):
        value = hop.get(key)
        if key in hop and (not isinstance(value, str) or not _COMPONENT_ID.match(value)):
            problems.append(f"ledger.hop: `{key}` must name a component id")
    if "from" in hop and hop["from"] != SUT_SERVICE:
        problems.append(f"ledger.hop: `from` must be {SUT_SERVICE}, the SUT the generators "
                        "send to")
    if "to" in hop and (len(extra_suts) != 1 or hop["to"] != extra_suts[0]):
        problems.append("ledger.hop: `to` must be the one further SUT service [configs] names"
                        + (f", {extra_suts[0]}" if len(extra_suts) == 1 else
                           f"; it names {', '.join(extra_suts) or 'none'}"))
    if ledger.get("sut_script") is not None:
        problems.append("ledger.hop and ledger.sut_script can't be combined")
    sources = ledger.get("source") if isinstance(ledger.get("source"), list) else []
    if any(isinstance(source, dict) and "sut_transforms" in source for source in sources):
        problems.append("ledger.hop and a source's `sut_transforms` can't be combined")
    return problems


def _parse_overlap(table, problems):
    """The `[overlap]` table as `{holder, instances, label}`, appending to `problems` what breaks
    its own rules: the three keys and no other; `holder` a service slug; `instances` two or more
    distinct service slugs, `logit` first, none the holder; and `label` a Prometheus label name.
    `_overlap_problems()` checks it against the ledger and the configs, `validate()` against the
    steps, and `service_problems()` against the stack."""
    if not isinstance(table, dict):
        problems.append("`overlap` must be a table, written [overlap]")
        return None
    for key in sorted(set(table) - OVERLAP_KEYS):
        problems.append(f"overlap: unknown key `{key}`")
    for key in sorted(OVERLAP_KEYS - set(table)):
        problems.append(f"overlap: missing `{key}`")
    holder, instances, label = table.get("holder"), table.get("instances"), table.get("label")
    if "holder" in table and (not isinstance(holder, str) or not _SERVICE_NAME.match(holder)):
        problems.append("overlap.holder must name the namespace holder's service: lowercase "
                        "letters, digits, `_`, or `-`")
    if "instances" in table:
        if (not isinstance(instances, list) or len(instances) < 2
                or not all(isinstance(i, str) and _SERVICE_NAME.match(i) for i in instances)
                or len(set(instances)) != len(instances)):
            problems.append("overlap.instances must list two or more distinct SUT services, such "
                            f"as [\"{SUT_SERVICE}\", \"{SUT_SERVICE}-b\"]")
            instances = None
        elif instances[0] != SUT_SERVICE:
            problems.append(f"overlap.instances must start with {SUT_SERVICE}, the SUT every "
                            "[[ledger.source]] names")
        elif holder in instances:
            problems.append("overlap.holder can't be an instance: a stopped instance would take "
                            "the namespace with it")
    if "label" in table and (not isinstance(label, str) or not _LABEL_NAME.match(label)):
        problems.append("overlap.label must be a Prometheus label name, such as \"soak_instance\"")
    return {"holder": holder, "instances": list(instances or []), "label": label}


def _yaml_seconds(text):
    """The first duration in `text` after a `delay:` key, in seconds, or None."""
    found = re.search(r"\bdelay\s*:\s*[\"']?([0-9][0-9a-z.]*)", text or "")
    if not found:
        return None
    try:
        return parse_duration(found.group(1))
    except ValueError:
        return None


def _uncommented(text):
    return "\n".join(re.sub(r"(^|\s)#.*$", "", line) for line in (text or "").splitlines())


def _overlap_problems(overlap, ledger, configs, extra_suts, directory):
    """What `[overlap]` breaks against the ledger and the configs. No `[[ledger.hop]]`, since
    every instance runs the whole pipeline; every source on a `udp` wire, the one the ledger
    judges an overlap on; a `[configs]` key other than `sut` and the generators only for an
    instance, which then runs its own config, else `sut`'s. Each instance's config sets
    `reuse_port: true` on every source's listener, a `shutdown.delay` above 0 that with the drain
    fits `docker stop`'s timeout, and `admin.socket` with no `admin.bind`, whose port the
    instances would share; it has a `set` component stamping `label`, which every source's
    `sut_transforms` lists; and two configs differ only in the lines that set `label`."""
    problems = []
    if ledger.get("hop") is not None:
        problems.append("[overlap] and [[ledger.hop]] can't be combined: every overlap instance "
                        "runs the whole pipeline")
    instances = overlap.get("instances") or []
    label = overlap.get("label")
    for service in extra_suts:
        if service not in instances[1:]:
            problems.append(f"configs.{service}: under [overlap] a key other than `sut` and the "
                            "sources' services names an instance's own config, and "
                            f"{service!r} isn't one of overlap.instances after {SUT_SERVICE}")
    sources = ledger.get("source") if isinstance(ledger.get("source"), list) else []
    for index, source in enumerate(sources):
        if isinstance(source, dict) and source.get("wire", DEFAULT_WIRE) != DEFAULT_WIRE:
            problems.append(f"ledger.source {index + 1}: under [overlap] a source's wire is udp, "
                            "the one whose loss at an instance's close ledger.overlap bounds")
    texts = {}
    for instance in instances:
        name = configs.get(instance if instance in extra_suts else SUT_CONFIG_KEY)
        if isinstance(name, str) and (directory / name).is_file():
            texts.setdefault(name, (instance, _uncommented((directory / name).read_text())))
    if not isinstance(label, str):
        return problems
    for name, (instance, text) in texts.items():
        where = f"overlap: {name} ({instance})"
        for index, source in enumerate(sources):
            listener = source.get("sut_listener") if isinstance(source, dict) else None
            if not isinstance(listener, str):
                continue
            if not any(re.search(r"\breuse_port\s*:\s*true\b", block)
                       for block in component_blocks(text, listener)):
                problems.append(f"{where}: listener {listener!r} needs `reuse_port: true`, or "
                                "the second instance's bind fails")
        delay = next((_yaml_seconds(block) for block in component_blocks(text, "shutdown")
                      if _yaml_seconds(block) is not None), None)
        if not delay:
            problems.append(f"{where}: needs a `shutdown.delay` above 0s, so a stopping instance "
                            "keeps reading while its flows move")
        elif delay + OVERLAP_DRAIN_S >= STOP_TIMEOUT_S:
            problems.append(f"{where}: `shutdown.delay` {format_duration(delay)} plus the "
                            f"{OVERLAP_DRAIN_S}s drain must be under docker stop's "
                            f"{STOP_TIMEOUT_S}s, or Docker kills the instance mid-drain")
        admin = "\n".join(component_blocks(text, "admin"))
        if not re.search(r"\bsocket\s*:", admin) or re.search(r"\bbind\s*:", admin):
            problems.append(f"{where}: admin needs `socket:` and no `bind:`; on a shared port a "
                            "probe could reach the other instance")
        setters = [component for component, body in _route_blocks(text, "set")
                   if re.search(rf"(^|[\s{{,]){re.escape(label)}\s*:", body)]
        if not setters:
            problems.append(f"{where}: no `set` component stamps {label!r}, so V can't be split "
                            "by instance")
        for index, source in enumerate(sources):
            chain = source.get("sut_transforms") if isinstance(source, dict) else None
            if setters and not (isinstance(chain, list) and set(chain) & set(setters)):
                problems.append(f"ledger.source {index + 1}: `sut_transforms` must list the set "
                                f"component stamping {label!r} ({', '.join(setters)}), so "
                                "ledger.edge judges it")
    others = list(texts.items())
    for name, (_, text) in others[1:]:
        first_name, (_, first) = others[0]

        def kept(body):
            return [line.rstrip() for line in body.splitlines()
                    if line.strip() and not re.search(rf"\b{re.escape(label)}\s*:", line)]

        if kept(first) != kept(text):
            problems.append(f"overlap: {name} and {first_name} differ in more than the lines "
                            f"setting {label!r}; overlap instances run the same config")
    return problems


def _overlap_step_problems(scenario):
    """What the steps break of `[overlap]`'s rules: on the holder only `netem` and `partition`,
    on an instance neither, and no two `INSTANCE_HOLD_ACTIONS` steps on different instances
    overlapping, since with every instance held nothing reads the port. A `[random]` fault on
    the holder or an instance is held to the first two; its draws never overlap."""
    overlap = scenario.overlap
    if overlap is None:
        return []
    holder, instances = overlap.get("holder"), set(overlap.get("instances") or [])
    problems = []
    entries = [(f"step {spec.index + 1} ({spec.action} on {spec.on})", spec.action, spec.on)
               for spec in scenario.steps]
    if scenario.random is not None:
        entries += [(f"random.fault {t.index + 1} ({t.action} on {t.on})", t.action, t.on)
                    for t in scenario.random.faults]
    for where, action, on in entries:
        if on == holder and action not in HOLDER_ACTIONS:
            problems.append(f"{where}: the holder takes {' and '.join(HOLDER_ACTIONS)} only; "
                            "any other fault on it takes every instance's network away")
        elif on in instances and action in HOLDER_ACTIONS:
            problems.append(f"{where}: an instance shares {holder}'s network namespace, so put "
                            f"{action} on {holder}")
    held = sorted((spec for spec in scenario.steps
                   if spec.on in instances and spec.action in INSTANCE_HOLD_ACTIONS),
                  key=lambda spec: spec.at)
    for i, first in enumerate(held):
        for second in held[i + 1:]:
            if second.on != first.on and second.at < first.at + first.for_:
                problems.append(f"steps {first.index + 1} and {second.index + 1}: "
                                f"{first.action} on {first.on} and {second.action} on "
                                f"{second.on} overlap; with every instance held nothing reads "
                                "the port")
    return problems


def _extra_sut_problems(services, ledger):
    """What the `[configs]` keys naming further SUT services break: each a slug, none `logit`,
    whose config is `[configs] sut`, and none whose `config_env()` is `SUT_CONFIG_ENV`, a
    source's, or another such key's."""
    problems = []
    sources = ledger.get("source") if isinstance(ledger.get("source"), list) else []
    taken = {SUT_CONFIG_ENV} | {config_env(source["service"]) for source in sources
                                if isinstance(source, dict)
                                and isinstance(source.get("service"), str)
                                and _SERVICE_NAME.match(source["service"])}
    for service in services:
        where = f"configs.{service}"
        if service == SUT_SERVICE:
            problems.append(f"{where}: the SUT {SUT_SERVICE}'s config is `{SUT_CONFIG_KEY}`")
        elif not _SERVICE_NAME.match(service):
            problems.append(f"{where}: a key other than `{SUT_CONFIG_KEY}` and the sources' "
                            "services names a further SUT service: lowercase letters, digits, "
                            "`_`, or `-`")
        elif config_env(service) in taken:
            problems.append(f"{where}: SUT service {service!r} would mount its config through "
                            f"{config_env(service)}, which the SUT, a source, or another SUT "
                            "service already uses")
        else:
            taken.add(config_env(service))
    return problems


# The compose variable holding the absolute path of `logit`'s config, set in `compose.env`.
SUT_CONFIG_ENV = "SOAK_SUT_CONFIG"
# The compose variable holding a stand-in target's shape, set in `compose.env`; compose.yaml
# passes it to standin.py's `--shape`.
STANDIN_SHAPE_ENV = "SOAK_STANDIN_SHAPE"
# A file wire's shared directory, `<run>/shared`, and the compose variable holding its path,
# which a scenario's override mounts into the writer and the SUT.
SHARED_DIR = "shared"
SHARED_DIR_ENV = "SOAK_SHARED_DIR"


def config_env(service):
    """The compose variable holding the absolute path of `service`'s config, a generator's or a
    further SUT's, set in `compose.env`: `SOAK_GENERATOR_CONFIG` for `generator`,
    `SOAK_GENERATOR_LINES_CONFIG` for `generator-lines`, `SOAK_COLLECTOR_CONFIG` for
    `collector`."""
    return "SOAK_" + re.sub(r"[^A-Z0-9]", "_", service.upper()) + "_CONFIG"


def _parse_target(table, problems):
    """The `[target]` table as `{kind, name, env}`, plus `topic` for Kafka and `shape` for a
    stand-in, appending to `problems` what breaks its rules: an unknown key or kind; for an
    external target, a `name` that isn't a slug, an `env` that isn't a list of distinct variable
    names compose.yaml passes, or a `topic`; for a Kafka target, a missing or illegal `topic`, a
    `name` that isn't a slug, or an `env`; for a stand-in target, a `name` that isn't a slug, an
    `env`, a `topic`, or a `shape` outside `STANDIN_SHAPES`; for the local target, any `name`,
    `env`, or `topic`; and a `shape` on any target but a stand-in."""
    local = {"kind": LOCAL_TARGET, "name": LOCAL_TARGET, "env": []}
    if not isinstance(table, dict):
        problems.append("`target` must be a table, written [target]")
        return local
    for key in sorted(set(table) - TARGET_KEYS):
        problems.append(f"target: unknown key `{key}`")
    kind = table.get("kind", LOCAL_TARGET)
    if kind not in TARGET_KINDS:
        problems.append(f"target.kind must be {LOCAL_TARGET!r}, {EXTERNAL_TARGET!r}, "
                        f"{KAFKA_TARGET!r}, or {STANDIN_TARGET!r}, got {kind!r}")
        return local
    if kind != STANDIN_TARGET and "shape" in table:
        problems.append("target.shape is for a standin target")
    if kind == LOCAL_TARGET:
        for key in sorted({"name", "env", "topic"} & set(table)):
            problems.append(f"target.{key} is for an external or kafka target; the local "
                            f"target is {LOCAL_TARGET}")
        return local
    if kind == STANDIN_TARGET:
        shape = table.get("shape", STANDIN_DEFAULT_SHAPE)
        if shape not in STANDIN_SHAPES:
            problems.append(f"target.shape must be one of {', '.join(STANDIN_SHAPES)}, got "
                            f"{shape!r}")
            shape = STANDIN_DEFAULT_SHAPE
        if "env" in table:
            problems.append("target.env is for an external target; the standin target runs "
                            "inside the stack and needs no credentials")
        if "topic" in table:
            problems.append("target.topic is for a kafka target")
        name = table.get("name", STANDIN_TARGET)
        if not isinstance(name, str) or not _TARGET_NAME.match(name):
            problems.append("target.name must be lowercase letters, digits, `_`, or `-`")
            name = STANDIN_TARGET
        return {"kind": kind, "name": name, "env": [], "shape": shape}
    if kind == KAFKA_TARGET:
        if "env" in table:
            problems.append("target.env is for an external target; the kafka target runs "
                            "inside the stack and needs no credentials")
        name = table.get("name", KAFKA_TARGET)
        if not isinstance(name, str) or not _TARGET_NAME.match(name):
            problems.append("target.name must be lowercase letters, digits, `_`, or `-`")
            name = KAFKA_TARGET
        topic = table.get("topic")
        if topic is None:
            problems.append("target: a kafka target needs `topic`, the topic the SUT's "
                            "kafka_out writes and the ledger reads back")
            topic = ""
        elif not isinstance(topic, str) or not _TOPIC_NAME.match(topic) or topic in (".", ".."):
            problems.append("target.topic must be a legal Kafka topic name: ASCII letters, "
                            "digits, `.`, `_`, or `-`, up to 249 characters")
            topic = ""
        return {"kind": kind, "name": name, "env": [], "topic": topic}
    if "topic" in table:
        problems.append("target.topic is for a kafka target")
    name = table.get("name")
    if not isinstance(name, str) or not _TARGET_NAME.match(name):
        problems.append("target.name must be lowercase letters, digits, `_`, or `-`, such as "
                        "\"datadog\"")
        name = EXTERNAL_TARGET
    env = table.get("env", [])
    if not isinstance(env, list) or not all(isinstance(v, str) for v in env):
        problems.append("target.env must be an array of environment variable names")
        env = []
    for variable in env:
        if not _ENV_NAME.match(variable):
            problems.append(f"target.env: {variable!r} isn't an environment variable name")
        elif variable not in EXTERNAL_ENV_PASSED:
            problems.append(f"target.env: compose.yaml doesn't pass {variable} to the SUT; it "
                            f"passes {', '.join(EXTERNAL_ENV_PASSED)}")
    if len(set(env)) != len(env):
        problems.append("target.env repeats a variable")
    return {"kind": kind, "name": name, "env": list(env)}


def read_env_file(path):
    """`{name: value}` from an env file, in plain or shell `export` form. Raises `OSError` when
    the file can't be read.

    The grammar, one assignment per line:

    - `KEY=value` or `export KEY=value`; space around the key and the value is dropped.
    - A value wrapped in one pair of matching single or double quotes loses that pair, once,
      and nothing inside it is unescaped or expanded.
    - Blank lines, `#` comment lines, and lines with no `=` are skipped. A `#` after a value is
      part of the value.
    - A key set twice takes its last value.
    """
    values = {}
    for line in Path(path).read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = re.sub(r"^export\s+", "", key.strip())
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
            value = value[1:-1]
        values[key] = value
    return values


def private_env_copy(env_file, names):
    """Writes the `names` `env_file` sets as plain `KEY=value` lines to a new file, mode 0600,
    in a new private temporary directory, and returns its path; `remove_private_env` removes
    both. Docker's and compose's `--env-file` read this copy, because neither reliably reads an
    `export` line or strips quotes. The copy holds credentials, so it lives outside the repo and
    the run directory, and a value never goes on a command line, where a process listing would
    show it."""
    values = read_env_file(env_file)
    directory = tempfile.mkdtemp(prefix=PRIVATE_ENV_PREFIX)
    # `mkstemp` creates the file 0600.
    fd, path = tempfile.mkstemp(suffix=".env", dir=directory)
    with os.fdopen(fd, "w") as out:
        for name in names:
            if name in values:
                out.write(f"{name}={values[name]}\n")
    return Path(path)


def remove_private_env(path):
    """Removes a `private_env_copy` file and its directory. A `None` or missing path is a
    no-op, and a path whose directory `private_env_copy` didn't name is left alone."""
    if path is None:
        return
    directory = Path(path).parent
    if directory.name.startswith(PRIVATE_ENV_PREFIX):
        shutil.rmtree(directory, ignore_errors=True)


def missing_env(scenario, env_file):
    """The `[target] env` variables `env_file` doesn't set, or sets empty, in the scenario's
    order; every one when the file can't be read. Returns names only, so no caller can print a
    value."""
    if not scenario.external or not scenario.target["env"]:
        return []
    try:
        values = read_env_file(env_file)
    except OSError:
        return list(scenario.target["env"])
    return [name for name in scenario.target["env"] if not values.get(name, "").strip()]


# Compose reads the private copy with dotenv rules: `$NAME` interpolates, ` #` starts a
# comment, quotes and `\` are parsed, and an unmatched quote fails `up` with an error that
# quotes the value. A value free of these reaches the SUT as written.
_COMPOSE_ALTERED = (("$", "a `$`"), ("'", "a single quote"), ('"', "a double quote"),
                    ("`", "a backtick"), ("\\", "a backslash"), ("#", "a `#`"))


def unsafe_env(scenario, env_file):
    """`NAME contains <kind of character>` for each `[target] env` variable whose value in
    `env_file` holds a character compose's env-file parser would alter or reject, in the
    scenario's order. Names only, so no caller can print a value; a file that can't be read
    yields nothing, since `missing_env` reports it."""
    if not scenario.external or not scenario.target["env"]:
        return []
    try:
        values = read_env_file(env_file)
    except OSError:
        return []
    problems = []
    for name in scenario.target["env"]:
        value = values.get(name, "")
        kinds = [label for char, label in _COMPOSE_ALTERED if char in value]
        if any(char.isspace() for char in value):
            kinds.append("whitespace")
        if kinds:
            problems.append(f"{name} contains {', '.join(kinds)}")
    return problems


UNSAFE_ENV_REASON = ("compose reads the env file with dotenv rules and would alter or reject "
                     "such a value")


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
    script = scenario.ledger.get("sut_script")
    if script is not None and (not isinstance(script, str) or not script):
        problems.append("ledger.sut_script must name the SUT's lua component")
    for spec in scenario.steps:
        where = f"step {spec.index + 1} ({spec.action or '?'} on {spec.on or '?'})"
        if spec.action not in ACTION_NAMES:
            hint = ""
            if spec.action in ("clear", "unpause", "start", "connect"):
                hint = "; reverts are scheduled from a fault's `for`, never written as steps"
            problems.append(f"{where}: unknown action `{spec.action}`{hint}")
        problems += _expected_event_problems(spec, where)
        problems += _respond_problems(spec, where, scenario.target.get("shape"))
        problems += _signal_problems(spec, where)
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
                    if first.action == "rename" and second.action == "signal":
                        continue
                    if "signal" in pair and pair & set(INSTANCE_HOLD_ACTIONS):
                        problems.append(
                            f"steps {first.index + 1} and {second.index + 1}: signal on "
                            f"{service} during its {(pair - {'signal'}).pop()}; the process "
                            "may not be running to receive it"
                        )
                    elif "netem" in pair and pair & set(NAMESPACE_FAULTS):
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
    problems += _overlap_step_problems(scenario)

    run_duration = scenario.duration if duration is None else duration
    problems += _rename_problems(scenario, run_duration)
    quiet_end = run_duration - scenario.cooldown
    if quiet_end < scenario.warmup:
        problems.append(
            f"duration {format_duration(run_duration)} leaves no time between warmup "
            f"{format_duration(scenario.warmup)} and cooldown {format_duration(scenario.cooldown)}"
        )
    if scenario.max_duration is not None and run_duration > scenario.max_duration:
        problems.append(
            f"duration {format_duration(run_duration)} is longer than the scenario's "
            f"max_duration {format_duration(scenario.max_duration)}, the longest run its "
            "thresholds and configs hold"
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


def _expected_event_problems(spec, where):
    """What a step breaks of the `mark` and `exit` rules: `ready` on a `mark` only, one of
    `READY_WORDS`; `code` and `key` required on an `exit` and refused elsewhere."""
    problems = []
    if spec.ready is not None:
        if spec.action != "mark":
            problems.append(f"{where}: `ready` is for mark only")
        elif spec.ready not in READY_WORDS:
            problems.append(f"{where}: `ready` must be one of {', '.join(READY_WORDS)}, got "
                            f"{spec.ready!r}")
    if spec.action == "exit":
        if spec.code is None:
            problems.append(f"{where}: exit needs `code`, the exit code the SUT leaves")
        elif (not isinstance(spec.code, int) or isinstance(spec.code, bool)
              or not 0 <= spec.code <= 255):
            problems.append(f"{where}: `code` must be an integer from 0 to 255, got "
                            f"{spec.code!r}")
        if spec.key is None:
            problems.append(f"{where}: exit needs `key`, the self-log key of the ERROR line "
                            "that explains it")
        elif not isinstance(spec.key, str) or not spec.key:
            problems.append(f"{where}: `key` must be a self-log key such as "
                            "\"memory_limit_exceeded\"")
    else:
        for name in ("code", "key"):
            if getattr(spec, name) is not None:
                problems.append(f"{where}: `{name}` is for exit only")
    return problems


def _respond_problems(spec, where, shape=None):
    """What a step breaks of the `respond` rules: one of `status` (an integer from 100 to 599) and
    `answer` (one of `RESPOND_ANSWERS`), `body` a string with `status` only, `apply` true or false,
    `series_errors` a positive integer with a 2xx `status` and no `body` under the `datadog` shape
    only, none of them on another action; and `class` one of `FAULT_CLASSES`, on a `respond` or a
    `stop` only, and under the `datadog` shape required on a `respond` other than a 2xx and refused
    on a 2xx. Whether a `respond`'s or a classed `stop`'s `on` is a stand-in needs the role map:
    `service_problems()`."""
    problems = []
    if spec.action == "respond":
        has_status, has_answer = spec.status is not None, spec.answer is not None
        if has_status == has_answer:
            problems.append(f"{where}: respond needs one of `status` and `answer`")
        if has_status and (not isinstance(spec.status, int) or isinstance(spec.status, bool)
                           or not 100 <= spec.status <= 599):
            problems.append(f"{where}: `status` must be an integer from 100 to 599, got "
                            f"{spec.status!r}")
        if has_answer and spec.answer not in RESPOND_ANSWERS:
            problems.append(f"{where}: `answer` must be one of {', '.join(RESPOND_ANSWERS)}, got "
                            f"{spec.answer!r}")
        if spec.body is not None and (not has_status or not isinstance(spec.body, str)):
            problems.append(f"{where}: `body` is a string, and goes with `status` only")
        if spec.apply is not None and not isinstance(spec.apply, bool):
            problems.append(f"{where}: `apply` must be true or false")
        if spec.series_errors is not None:
            count = spec.series_errors
            if shape != DATADOG_SHAPE:
                problems.append(f"{where}: `series_errors` is for a standin target with shape "
                                f"{DATADOG_SHAPE!r}")
            if not isinstance(count, int) or isinstance(count, bool) or count < 1:
                problems.append(f"{where}: `series_errors` must be a positive integer, got "
                                f"{count!r}")
            if spec.body is not None:
                problems.append(f"{where}: `series_errors` writes the body, so it takes no "
                                "`body`")
            if not (isinstance(spec.status, int) and not isinstance(spec.status, bool)
                    and 200 <= spec.status < 300):
                problems.append(f"{where}: `series_errors` goes with a 2xx `status`")
        if shape == DATADOG_SHAPE:
            success = (isinstance(spec.status, int) and not isinstance(spec.status, bool)
                       and 200 <= spec.status < 300)
            if success and spec.fault_class is not None:
                problems.append(f"{where}: a 2xx isn't a fault, so it takes no `class`")
            elif not success and spec.fault_class is None:
                # `ledger.standin` expects a request's dropped records from its step's class
                # alone, so a step with none would leave a rejection unexpected.
                problems.append(f"{where}: under the datadog shape a respond other than a 2xx "
                                "needs `class`, the class datadog_out must read it as")
    else:
        for name in RESPOND_KEYS:
            if getattr(spec, name) is not None:
                problems.append(f"{where}: `{name}` is for respond only")
    if spec.fault_class is not None:
        if spec.action not in CLASSED_ACTIONS:
            problems.append(f"{where}: `class` is for {' and '.join(CLASSED_ACTIONS)} only")
        elif spec.fault_class not in FAULT_CLASSES:
            problems.append(f"{where}: `class` must be one of {', '.join(FAULT_CLASSES)}, got "
                            f"{spec.fault_class!r}")
    return problems


def _signal_problems(spec, where):
    """What a step breaks of the `signal` and `rename` keys: `signal`, one of `SIGNALS`, required
    on a `signal` and refused elsewhere; `path` and `to` required on a `rename` and refused
    elsewhere. `_rename_problems()` checks a rename against its service's config."""
    problems = []
    if spec.action == "signal":
        if spec.signal is None:
            problems.append(f"{where}: signal needs `signal`, such as \"HUP\"")
        elif spec.signal not in SIGNALS:
            problems.append(f"{where}: `signal` must be one of {', '.join(SIGNALS)}, got "
                            f"{spec.signal!r}")
    elif spec.signal is not None:
        problems.append(f"{where}: `signal` is for signal only")
    if spec.action == "rename":
        for name in ("path", "to"):
            value = getattr(spec, name)
            if value is None:
                problems.append(f"{where}: rename needs `{name}`, a file name in "
                                f"{SHARED_MOUNT}")
            elif (not isinstance(value, str) or not value or "/" in value
                  or value in (".", "..")):
                problems.append(f"{where}: `{name}` must be a bare file name in "
                                f"{SHARED_MOUNT}, with no `/`, got {value!r}")
        if spec.path is not None and spec.path == spec.to:
            problems.append(f"{where}: `to` must differ from `path`")
    else:
        for name in ("path", "to"):
            if getattr(spec, name) is not None:
                problems.append(f"{where}: `{name}` is for rename only")
    return problems


def stdio_shared_targets(text):
    """`{file name: component id}` for each `stdio_out` in config `text` whose `target:` is a
    file directly in `SHARED_MOUNT`."""
    targets = {}
    for component, body in _route_blocks(text, "stdio_out"):
        found = re.search(r"\btarget\s*:\s*[\"']?([^\"'\s,}]+)", body)
        if not found:
            continue
        head, _, name = found.group(1).rpartition("/")
        if head == SHARED_MOUNT and name:
            targets[name] = component
    return targets


def service_config_key(scenario, service):
    """The `[configs]` key of `logit` service `service`'s config, or None for a service with
    none."""
    if service in scenario.configs and service != SUT_CONFIG_KEY:
        return service
    if service in scenario.suts:
        return SUT_CONFIG_KEY
    return None


def _rename_problems(scenario, duration):
    """What the `rename` steps break: each `path` a `stdio_out` target in `SHARED_MOUNT` in its
    service's config, each `to` none of that config's targets there and distinct across
    renames; a `signal` with `signal = "HUP"` on the same service starting inside each rename's
    `for`, after its start, so the target reopens while the step holds; and each rename once
    per run of `duration`, since a second cycle's rename would find its `to` taken."""
    problems = []
    renames = [spec for spec in scenario.steps if spec.action == "rename"]
    seen_to = {}
    for spec in renames:
        where = f"step {spec.index + 1} (rename on {spec.on or '?'})"
        key = service_config_key(scenario, spec.on)
        text = ""
        if key is None:
            problems.append(f"{where}: {spec.on} has no config in [configs], so no stdio_out "
                            "target to rename")
        else:
            try:
                text = scenario.config_path(key).read_text()
            except OSError:
                text = ""
        targets = stdio_shared_targets(text)
        if key is not None and isinstance(spec.path, str) and spec.path not in targets:
            problems.append(f"{where}: `path` {spec.path!r} isn't a stdio_out target in "
                            f"{SHARED_MOUNT} in {spec.on}'s config; its targets there are "
                            f"{', '.join(sorted(targets)) or 'none'}")
        if isinstance(spec.to, str):
            if spec.to in targets:
                problems.append(f"{where}: `to` {spec.to!r} is a stdio_out target in "
                                f"{spec.on}'s config, which would write into it again")
            if spec.to in seen_to:
                problems.append(f"{where}: `to` {spec.to!r} repeats step "
                                f"{seen_to[spec.to] + 1}'s, and a rename onto it would replace "
                                "that file")
            else:
                seen_to[spec.to] = spec.index
        if not any(other.action == "signal" and other.on == spec.on
                   and other.signal == "HUP" and spec.at < other.at < spec.at + spec.for_
                   for other in scenario.steps):
            problems.append(f"{where}: needs a signal = \"HUP\" step on {spec.on} starting "
                            "inside its `for`, which makes the target reopen its path")
    if renames and scenario.random is None and scenario.cycle > 0:
        counts = {}
        for step in expand(scenario, duration):
            if step.action == "rename":
                counts[step.spec_index] = counts.get(step.spec_index, 0) + 1
        for index, count in sorted(counts.items()):
            if count > 1:
                problems.append(f"step {index + 1} (rename): runs {count} times in "
                                f"{format_duration(duration)}, and a second rename would find "
                                "its `to` taken; set max_duration to one cycle's run")
    return problems


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
        if template.action in EXPECTED_EVENT_ACTIONS:
            problems.append(f"{where}: {template.action} names an expected event at a fixed "
                            "time, and a [random] schedule draws faults; write it as a [[step]]")
        if template.action == "respond":
            problems.append(f"{where}: respond takes a status or an answer, which a [random] "
                            "fault can't set; write it as a [[step]]")
        if template.action in NO_WINDOW_ACTIONS:
            problems.append(f"{where}: {template.action} opens no fault window and takes keys a "
                            "[random] fault can't set; write it as a [[step]]")
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
    """Every rule an expanded schedule breaks: known actions, each fault inside warmup's end
    and cooldown's start, no two faults overlapping on one container, no netem during a
    namespace fault, and, for a random schedule, no two faults overlapping anywhere, each after
    a gap of at least `gap.min`. `service_problems()` checks the services."""
    problems = []
    quiet_end = duration - scenario.cooldown
    for step in steps:
        where = f"step {step.id} ({step.action} on {step.on})"
        if step.action not in ACTION_NAMES:
            problems.append(f"{where}: unknown action")
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
        if exp.window == RUN_WINDOW:
            if exp.step is not None:
                problems.append(f"{where}: window `run` spans the whole run and takes no "
                                "`step`")
        elif isinstance(exp.step, int) and not isinstance(exp.step, bool):
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


def roles_from_config(text):
    """`{service: role}` from `docker compose config --format json`'s output, a service with no
    `soak.role` label mapped to None. Raises `ValueError` for output that isn't that JSON."""
    services = json.loads(text).get("services")
    if not isinstance(services, dict):
        raise ValueError("no `services` in the compose config")
    return {name: ((spec or {}).get("labels") or {}).get(ROLE_LABEL)
            for name, spec in services.items()}


def network_modes_from_config(text):
    """`{service: network_mode}` from `docker compose config --format json`'s output, for each
    service that sets one."""
    services = json.loads(text).get("services") or {}
    return {name: (spec or {}).get("network_mode") for name, spec in services.items()
            if (spec or {}).get("network_mode")}


def base_roles(scenario):
    """`BASE_ROLES` as the driver would resolve it for `scenario` with no override: without the
    backends, which the `local` profile holds, for any target but the local one, with
    `KAFKA_ROLES`, which the `kafka` profile holds, for a Kafka target, and with
    `STANDIN_ROLES`, which the `standin` profile holds, for a stand-in target."""
    roles = {name: role for name, role in BASE_ROLES.items()
             if scenario.has_vm or role != BACKEND_ROLE}
    if scenario.kafka:
        roles.update(KAFKA_ROLES)
    if scenario.standin:
        roles.update(STANDIN_ROLES)
    return roles


def logit_services(roles):
    """The services that run `logit`, every SUT service and every generator, sorted."""
    return sorted(name for name, role in roles.items() if role in LOGIT_ROLES)


def backends(roles):
    return sorted(name for name, role in roles.items() if role == BACKEND_ROLE)


def generators(roles):
    return sorted(name for name, role in roles.items() if role == GENERATOR_ROLE)


def role_problems(roles, suts=(SUT_SERVICE,), holder=None):
    """What a role map breaks: every service labeled with a known role, the SUT services
    `suts` (`Scenario.suts`: `SUT_SERVICE` and each further one `[configs]` names, or the
    overlap's instances) and no other with the role `sut`, at least one generator, and the
    `[overlap]` `holder` and no other service with the role `netns`."""
    problems = []
    netns = sorted(n for n, r in roles.items() if r == NETNS_ROLE)
    if holder is None and netns:
        problems.append(f"compose service {', '.join(netns)}: the {NETNS_ROLE} role is an "
                        "[overlap] holder's, and the scenario has no [overlap]")
    elif holder is not None and netns != [holder]:
        problems.append(f"overlap.holder {holder} must be the stack's one {NETNS_ROLE} service, "
                        f"and the stack's are {', '.join(netns) or 'none'}")
    for name, role in sorted(roles.items()):
        if role not in ROLES:
            problems.append(f"compose service {name}: label {ROLE_LABEL} must be one of "
                            f"{', '.join(ROLES)}, got {role!r}")
    holders = sorted(n for n, r in roles.items() if r == SUT_ROLE)
    if holders != sorted(suts):
        if list(suts) == [SUT_SERVICE]:
            problems.append(f"the stack needs one {SUT_ROLE} service, {SUT_SERVICE}, and has "
                            f"{', '.join(holders) or 'none'}; a further {SUT_ROLE} service "
                            "needs its own [configs] key")
        else:
            problems.append(f"the stack needs the {SUT_ROLE} services {SUT_SERVICE} and the "
                            f"[configs] keys {', '.join(suts[1:])}, and has "
                            f"{', '.join(holders) or 'none'}")
    if not generators(roles):
        problems.append(f"the stack needs at least one {GENERATOR_ROLE} service, and has none")
    return problems


def service_problems(scenario, roles, modes=None):
    """Every rule that needs the stack's services, against `roles` (`{service: role}`): the
    role map's own rules; each step's and random fault's `on` a service of the stack, with a
    `kill` or an `exit` on a SUT service only, and a `respond`, or a `stop` with a `class`, on a
    stand-in only; each `[[expect]]` service one that runs `logit`; for a Kafka target, the
    broker `kafka`; for a stand-in target, the stand-in `standin`; each `[[ledger.source]]`
    service a generator, and every generator a source's; and each local sink's `backend` a
    backend service. With `modes`, `{service: network_mode}` from the merged compose config, each
    overlap instance is in its holder's namespace."""
    problems = role_problems(roles, scenario.suts,
                             None if scenario.overlap is None else scenario.overlap["holder"])
    if scenario.overlap is not None and modes is not None:
        want = f"service:{scenario.overlap['holder']}"
        for instance in scenario.overlap["instances"]:
            if modes.get(instance) != want:
                problems.append(f"overlap instance {instance} needs network_mode \"{want}\", so "
                                f"it shares the holder's address, and has "
                                f"{modes.get(instance) or 'none'}")
    no_backend = ("; an external target's stack runs no backend" if scenario.external else
                  "; a kafka target's stack runs the broker and no backend" if scenario.kafka
                  else "; a standin target's stack runs the stand-in and no backend"
                  if scenario.standin else "")

    def on_problems(on, action, where, reason=True, classed=False):
        if on not in roles:
            return [f"{where}: unknown service `{on}`{no_backend}"]
        if action in NO_WINDOW_ACTIONS and roles[on] not in LOGIT_ROLES:
            return [f"{where}: {action} is for a service that runs logit; the stack's are "
                    f"{', '.join(logit_services(roles))}"]
        if action in SUT_ONLY_ACTIONS and roles[on] != SUT_ROLE:
            why = f": {SUT_ONLY_ACTIONS[action]}" if reason else ""
            return [f"{where}: {action} is for {' or '.join(scenario.suts)} only{why}"]
        if (action == "respond" or classed) and roles[on] != STANDIN_ROLE:
            what = "respond" if action == "respond" else f"`class` on a {action}"
            standins = sorted(n for n, r in roles.items() if r == STANDIN_ROLE)
            return [f"{where}: {what} is for a {STANDIN_ROLE} service; the stack's are "
                    f"{', '.join(standins) or 'none'}"]
        return []

    for spec in scenario.steps:
        problems += on_problems(spec.on, spec.action,
                                f"step {spec.index + 1} ({spec.action or '?'} on {spec.on or '?'})",
                                classed=spec.fault_class is not None)
    if scenario.random is not None:
        for template in scenario.random.faults:
            problems += on_problems(template.on, template.action,
                                    f"random.fault {template.index + 1} ({template.action or '?'} "
                                    f"on {template.on or '?'})", reason=False)
    runs = logit_services(roles)
    for exp in scenario.expectations:
        if exp.service not in runs:
            problems.append(f"expect {exp.index + 1} ({exp.name or '?'}): unknown service "
                            f"`{exp.service}`; telemetry comes from {' or '.join(runs)}")
    for row, services in scenario.expected.items():
        for service in services:
            if service not in runs:
                problems.append(f"expected.{row}: unknown service `{service}`; {row} judges "
                                f"{' and '.join(runs)}")
    if scenario.kafka and roles.get(KAFKA_TARGET) != BROKER_ROLE:
        problems.append(f"a kafka target reads its topic from the {BROKER_ROLE} service "
                        f"`{KAFKA_TARGET}`, which the stack doesn't run")
    if scenario.standin and roles.get(STANDIN_TARGET) != STANDIN_ROLE:
        problems.append(f"a standin target reads its V from the {STANDIN_ROLE} service "
                        f"`{STANDIN_TARGET}`, which the stack doesn't run")
    sourced = []
    for index, source in enumerate(scenario.sources):
        service = source.get("service")
        sourced.append(service)
        if roles.get(service) != GENERATOR_ROLE:
            problems.append(f"ledger.source {index + 1}: service `{service}` isn't a "
                            f"{GENERATOR_ROLE} service; the stack's are "
                            f"{', '.join(generators(roles)) or 'none'}")
    for service in generators(roles):
        if service not in sourced:
            problems.append(f"generator service {service} has no [[ledger.source]], so the "
                            "ledger would read its lines with no G behind them")
    for index, sink in enumerate(scenario.sinks):
        backend = sink.get("backend")
        if backend is not None and roles.get(backend) != BACKEND_ROLE:
            problems.append(f"ledger.sink {index + 1} ({sink.get('id')}): backend `{backend}` "
                            f"isn't a backend service; the stack's are "
                            f"{', '.join(backends(roles)) or 'none'}")
    return problems


def check_services(scenario, roles, modes=None):
    """Raises `ScenarioError` listing every `service_problems()` rule broken."""
    problems = service_problems(scenario, roles, modes)
    if problems:
        raise ScenarioError(problems)


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
                    ready=spec.ready, code=spec.code, key=spec.key, status=spec.status,
                    body=spec.body, apply=spec.apply, answer=spec.answer,
                    series_errors=spec.series_errors, fault_class=spec.fault_class,
                    signal=spec.signal, path=spec.path, to=spec.to,
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
