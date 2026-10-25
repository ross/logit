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
V comes from the topic's distinct records, read back by the driver in the sink's `format`
and deduplicated on their raw bytes, and under `otlp_proto` on each data point's series and time
too (topic.py), so `ledger.egress` and `ledger.summary` judge Ab − V against it, and a
surplus FAILs as on any target. A Kafka target has one sink, whose V the topic is.
`ledger.topic` judges the topic's distinct record count against the records the sink saw
accepted, and its duplicate copies and repeated data points per life against
`EPOCH_BUMP_REASONS`; it SKIPs for every other target.

The ledger follows each `[[ledger.sink]]`. A row about a sink (`progress`, `identity.sink`,
`recovery`, `ledger.egress`, `ledger.windows`, `ledger.replay`, `ledger.sent`) keeps its id,
reports one line per sink, and takes the worst status. Under `split = "copy"` each sink holds a
full copy of `aggregate`'s output, so each sink's V is judged alone; under `split = "route"` the
sinks' V add up to it, so the egress gap is judged on their sum. That holds because `load()`
refuses `split = "route"` over a `route` that can send one event to several targets (a
list-valued `routes:` value) and over any `route_signals`, so each event reaches
one sink. Those routers run under `split = "target"`: each sink sits behind its own target and
`aggregate`, the listeners' rows end at the router (`[ledger] sut_router`), the router balances
as received + copied − discarded == its forward edge's sends + its targets' (`router_balance`),
and each sink's rows start at its own target (`branch_fed`), so each sink's V is judged alone.

The ledger also follows each `[[ledger.source]]`, a generator and the SUT listener it reaches
over the source's `wire` (`WIRES`), which names the counters each symbol reads.
`ledger.wire` reports one line per source, each judged against the fault windows that affect
its wire, of its own generator and of every service that isn't a generator (`affects_source`);
on TCP only a window that can reset the connection is reported, on HTTP only one that can lose
the SUT's unexported counts, and every other one is judged (`wire_judgment`). `ledger.intake`
judges each listener's final life alone. From the edge into
`aggregate` on, every row sums the listeners' symbols, since their events merge there; a
source's `sut_transforms` are judged hop by hop in `ledger.edge`, each node's discards
(`events.discarded`, T_disc) counted there, and their sum leaves the residual, the stopped-life
egress gap, `ledger.summary`'s path term, and the end-to-end bound as a script's discards do
(`Ledger.discarded`). A source's `fan_out` node emits k events per event, so with one the rows
that compare the listeners' symbols with `aggregate`'s or V read them carried through the chain,
W′ (`Ledger.through`), which takes the discards in. `recovery` judges the
listeners' summed ingest against the generators' summed warmup rate.

A run against a stand-in target (`[target] kind = "standin"`) has no VictoriaMetrics either: V
is what the stand-in's record says it stored (standin.py), in the export's shape, so
`ledger.egress` and `ledger.summary` judge it as they judge an export, and `ledger.standin`
judges the record against the sink's telemetry and each classed step's effect. A batch dropped
`rejected` inside a `respond` step that expects the `rejected` class counts its gap and doesn't
FAIL (`Ledger.cued_rejections`); outside one it FAILs as on any target. Under the `remote_write`
shape `ledger.sent` SKIPs. Under the `datadog` shape V is the distinct stored points' delta
sum, judged as a Kafka topic's is (a negative gap is a surplus), and `ledger.sent` judges the
sink as on an external target, excusing a refusal line, a rejected batch, or a rejected or
oversize record drop inside a `respond` step that explains it (`_sent_one`). Under the
`influxdb` shape V is the stored delta points' sum, one per `(series, timestamp)` since
InfluxDB overwrites a resend, judged as the `datadog` shape's is, and `ledger.sent` SKIPs.

A run can have several SUT services (`RunData.suts`, in chain order). Each has its own process
lives (`SutLives`): the listener rows read the first service's, and every row from `aggregate`
on reads the last service's, which holds it and the sinks, with V split on its lives. With one
SUT, `logit`, both are the same. Two SUT services are joined by a `[[ledger.hop]]`
(docs/plans/soak-harness.md's "The native hop (W10b)"): the far side's rows read its
`logit_in`'s events sent where the listeners' W − D − B − X would be, `ledger.hop` judges the
hop on run totals, since a life of one service isn't a life of the other, and
`ledger.summary` bounds V end to end. Without a hop two services SKIP every ledger row.

A rolling overlap (`[overlap]`) runs several instances of one pipeline on one shared port: the
rows about a process life judge each instance on its own lives, and the wire, `progress`,
`recovery`, and `ledger.overlap` judge them together ("a rolling overlap" below).

A `signal`, `rename`, `rotate`, or `probe` step opens no fault window (`RunData.events`): only
`[[expect]]`, `file.reopen`, `tls.served`, and `self_log`'s excusal of a broken rotation's
failed reload read it, and every other row judges straight through it.

`rss_slope` and `fd_slope` judge each `logit` service on its own line, and a scenario's
`[expected]` table can set the verdict a service should get on either (`judge_services`).
`rss_slope` alone widens each fault window by the scenario's `rss_settle` past
`recovery_bound` (`RunData.rss_windows`): a backlog's memory comes back in steps over minutes,
and a fit over the plateaus beside the pre-fault baseline slopes upward with no leak.
"""

import bisect
import copy
import json
import math
import re
import statistics
from dataclasses import dataclass, field
from pathlib import Path

from . import collect, faults, standin, telemetry, topic, vm
from .scenario import (DEFAULT_RSS_SETTLE_S, DEFAULT_WIRE, EXPECT_REDUCERS, FILE_WIRE, EXTERNAL_TARGET, GENERATOR_ROLE,
                       HTTP_WIRE, KAFKA_TARGET, LOCAL_TARGET, LOGIT_ROLES, NO_WINDOW_ACTIONS,
                       IDP_ROLE, RUN_WINDOW, SHARED_DIR, SHARED_MOUNT, STANDIN_ROLE, STANDIN_TARGET,
                       SUT_CONFIG_ENV, SUT_CONFIG_KEY, SUT_SERVICE, TARGET_SPLIT,
                       component_blocks, config_env,
                       listener_wire, parse_duration, source_ids, source_pairs,
                       stdio_shared_targets, tls_reload_polls)

PASS, WARN, FAIL, SKIP = "PASS", "WARN", "FAIL", "SKIP"
_ORDER = {PASS: 0, SKIP: 0, WARN: 1, FAIL: 2}
# The self-log keys of a TLS reload that kept the previous files, and of one that loaded
# (crates/logit-pipeline/src/tls.rs, `Watched::check`).
TLS_RELOAD_FAILED_KEY = "tls_reload_failed"
TLS_RELOADED_KEY = "tls_reloaded"
# The self-log key of a failed OAUTHBEARER token refresh that kept the cached token
# (crates/logit-outputs/src/kafka/sasl/oauth.rs, `Endpoint::token`).
TOKEN_REFRESH_FAILED_KEY = "token_refresh_failed"


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
# How late the driver's tick may run before the run is `stalled` (driver.py's `STALL_S`).
STALL_S = 120
# The `aborted` phase's reason when the driver ended the schedule at a stall, and the start of
# each line `run` writes for a stall, which `stalled()` reads.
STALL_ABORT_REASON = "the schedule stopped at a stall, and the end sequence ran"
STALL_LINE = "stalled:"
POLL_SLACK_S = 5


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
    # A `mark`'s `ready` word, an `exit`'s `code` and `key`, a `respond`'s status or answer,
    # and the fault class a step expects; None elsewhere.
    ready: object = None
    code: object = None
    key: object = None
    status: object = None
    answer: object = None
    fault_class: object = None
    series_errors: object = None
    # A `respond`'s `count`: the writes its cue answered before it was spent; None for a cue that
    # answers every write in its span.
    count: object = None
    # Whether the fault can lose lines on a `tcp`, a `file`, or an `http` source's wire, as
    # `affects_udp_ingress` is for a `udp` one (faults.py's `INGRESS_KEYS`).
    affects_tcp_ingress: bool = False
    affects_file_ingress: bool = False
    affects_http_ingress: bool = False
    # A partition's `{network: IP address}` before its disconnect and after its reconnect.
    ips: object = None
    ips_after: object = None
    # A `signal`'s signal, and a `rename`'s file and new name; None elsewhere.
    signal: object = None
    path: object = None
    to: object = None
    # When the apply finished, which for a `signal` is when Docker had sent it.
    applied: object = None
    # A `rotate`'s certificate, days, generation, and whether it was issued broken, with the
    # serial and `notAfter` it issued; a `probe`'s certificate and port, with the serial and
    # `notAfter` the listener served and whether they verified against the CA. None elsewhere.
    cert: object = None
    days: object = None
    broken: object = None
    gen: object = None
    port: object = None
    serial: object = None
    not_after: object = None
    verified: object = None

    def affects(self, wire):
        return bool(getattr(self, faults.INGRESS_KEYS[wire]))

    def ip_text(self):
        """A partition's address before and after, as text: `IP a kept` or `IP a -> b`, `IP a
        -> unknown` with no record after the reconnect; "" for any other fault."""
        if self.ips is None:
            return ""
        before = ", ".join(sorted(ip for ip in self.ips.values() if ip)) or "none"
        if self.ips_after is None:
            return f"IP {before} -> unknown"
        after = ", ".join(sorted(ip for ip in self.ips_after.values() if ip)) or "none"
        return f"IP {before} kept" if self.ips == self.ips_after else f"IP {before} -> {after}"


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
        # A run directory from before `rss_settle` existed takes the default, 0 s, and scores as
        # it did then.
        self.rss_settle = float(self.resolved.get("rss_settle", DEFAULT_RSS_SETTLE_S))
        self.warmup = float(self.resolved.get("warmup", 0))
        self.thresholds = self.resolved.get("thresholds", {})
        self.ledger = self.resolved.get("ledger", {})
        self.sinks = [sink["id"] for sink in self.ledger.get("sink") or []]
        self.split = self.ledger.get("split", "copy")
        # `{service: role}` as the driver resolved it from compose.
        self.services = self.resolved.get("services") or {}
        self.logit_services = sorted(name for name, role in self.services.items()
                                     if role in LOGIT_ROLES)
        # The SUT services in chain order (`Scenario.suts`): the first holds every source's
        # listener, the last `sut_aggregate` and every sink. A run directory with no `suts` ran
        # `logit` alone.
        self.suts = list(self.resolved.get("suts") or [SUT_SERVICE])
        self.sut_in, self.sut_out = self.suts[0], self.suts[-1]
        # `[overlap]`: `{holder, instances, label}`, or None. Each instance runs the whole
        # pipeline, so the per-life rows read each through `instance_view()`.
        self.overlap = self.resolved.get("overlap") or None
        self.instances = list((self.overlap or {}).get("instances") or [])
        # The instance an `instance_view()` reads, or None for the run as a whole.
        self.overlap_instance = None
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
        self.standin = self.target.get("kind") == STANDIN_TARGET
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
        # `timeline` FAILs the action itself. A step of `NO_WINDOW_ACTIONS` opens no window, so
        # it goes in `events`, which only `[[expect]]`, `file.reopen`, `tls.served`, and
        # `self_log`'s broken-rotation excusal read.
        self.faults = []
        self.events = []
        applies = {}
        for record in self.timeline:
            if record.get("event") == "apply":
                applies[record["step"]] = record
            elif record.get("event") == "revert" and record["step"] in applies:
                apply = applies.pop(record["step"])
                if not apply.get("rc"):
                    self._held(apply).append(self._fault(apply, record["finished_at"], record))
        for apply in applies.values():
            if not apply.get("rc"):
                self._held(apply).append(self._fault(apply, self.end))

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

        # A stand-in target's record, its stdout, and V rebuilt from it for the one sink. A
        # delta sink's shape keeps the stored points as V's terms (`standin_counts`), with their
        # series in the export's shape: under `datadog` each distinct copy, under `influxdb`
        # each `(series, timestamp)`'s last write.
        self.standin_requests, self.standin_cues, self.standin_unparsed = [], [], 0
        self.standin_text = None
        self.standin_store = ([], 0, [], [])
        self.standin_shape = (self.target.get("shape", standin.REMOTE_WRITE) if self.standin
                              else None)
        self.standin_counts = None
        self.standin_datadog = self.standin_shape == standin.DATADOG
        self.standin_influxdb = self.standin_shape == standin.INFLUXDB
        if self.standin:
            service = next((name for name, role in self.services.items()
                            if role == STANDIN_ROLE), STANDIN_TARGET)
            try:
                self.standin_text = (self.run_dir / "logs" / f"{service}.stdout").read_text()
            except OSError:
                self.standin_text = None
            (self.standin_requests, self.standin_cues,
             self.standin_unparsed) = standin.read_record(self.standin_text)
            if self.standin_shape == standin.DATADOG:
                counts = standin.stored_counts(self.standin_requests)
                self.standin_counts = counts
                self.standin_store = (counts.series, 0, counts.conflicts, counts.undecodable)
            elif self.standin_shape == standin.INFLUXDB:
                counts = standin.stored_points(self.standin_requests)
                self.standin_counts = counts
                self.standin_store = (counts.series, len(counts.rewrites), counts.conflicts,
                                      counts.undecodable)
            else:
                self.standin_store = standin.stored_series(self.standin_requests)
            for sink in self.sinks:
                self.vm_series[sink] = self.standin_store[0]

    def _held(self, apply):
        return self.events if apply.get("action") in NO_WINDOW_ACTIONS else self.faults

    @staticmethod
    def _fault(apply, end, revert=None):
        return Fault(
            step=apply["step"], action=apply["action"], on=apply["on"],
            start=apply["started_at"], end=end,
            affects_udp_ingress=apply.get("affects_udp_ingress", False),
            affects_egress=apply.get("affects_egress", False),
            ready=apply.get("ready"), code=apply.get("code"), key=apply.get("key"),
            status=apply.get("status"), answer=apply.get("answer"),
            fault_class=apply.get("class"), series_errors=apply.get("series_errors"),
            count=apply.get("count"),
            affects_tcp_ingress=apply.get("affects_tcp_ingress", False),
            affects_file_ingress=apply.get("affects_file_ingress", False),
            affects_http_ingress=apply.get("affects_http_ingress", False),
            ips=apply.get("ips"), ips_after=(revert or {}).get("ips_after"),
            signal=apply.get("signal"), path=apply.get("path"), to=apply.get("to"),
            applied=apply.get("finished_at"),
            cert=apply.get("cert"), days=apply.get("days"), broken=apply.get("broken"),
            gen=apply.get("gen"), port=apply.get("port"), serial=apply.get("serial"),
            not_after=apply.get("not_after"), verified=apply.get("verified"),
        )

    def offset(self, t):
        return "?" if t is None or self.t0 is None else f"{t - self.t0:+.0f}s"

    def sut_phase(self, name, service):
        """The last `name` phase the end sequence recorded for SUT `service`. A phase with no
        `service` is from a run with one SUT."""
        found = None
        for record in self.timeline:
            if (record.get("event") == "phase" and record.get("phase") == name
                    and record.get("service", service) == service):
                found = record
        return found

    def exit_windows(self, service):
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

    def rss_windows(self):
        """`windows()` widened by `rss_settle` past `recovery_bound`, for `rss_slope` only."""
        return [(start, end + self.rss_settle) for start, end in self.windows()]

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

    def steady_span(self, points, windows=None):
        """Seconds between consecutive [(ts, value)] points with no fault window, or none of
        `windows` when given, between them: a life's samples can sit on both sides of a fault
        that took most of the run."""
        windows = self.windows() if windows is None else windows
        return sum(b - a for (a, _), (b, _) in zip(points, points[1:])
                   if not any(start < b and end > a for start, end in windows))

    def in_window(self, t, windows):
        return any(start <= t <= end for start, end in windows)

    def steady(self, t, after_warmup=False, windows=None):
        """Whether `t` is inside the judged steady state: after the timeline's zero (and the
        warmup when asked), before the end sequence, and outside every fault window, or every
        one of `windows` when given."""
        if self.t0 is None or t is None:
            return False
        if t < self.t0 + (self.warmup if after_warmup else 0) or t >= self.end:
            return False
        return not self.in_window(t, self.windows() if windows is None else windows)

    def quiet_intervals(self, start, windows=None):
        """[(start, end)] spans from `start` to the end sequence outside every fault window, or
        every one of `windows` when given."""
        spans = [(start, self.end)]
        for w_start, w_end in sorted(self.windows() if windows is None else windows):
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
    interrupted, stalled (its tick ran far late, as across a host suspend), or missing either
    phase. The driver still exits 130 when interrupted."""
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
    stalls = [(record.get("since", 0), record.get("late_s", 0)) for record in data.timeline
              if record.get("event") == "phase" and record.get("phase") == "stalled"]
    if not stalls and data.t0 is not None and not data.phases.get("start", {}).get(
            "marks_stalls"):
        # A run directory from before the driver recorded the phase: the watchdog's 5 s
        # inspections stop for the span. The end sequence inspects once as it begins.
        polled = sorted({r["t"] for r in data.watchdog
                         if r.get("t") and r["t"] <= data.end + POLL_SLACK_S})
        stalls = [(a - data.t0, b - a) for a, b in zip(polled, polled[1:])
                  if b - a > STALL_S]
    for since, late in stalls:
        lines.append(f"{STALL_LINE} the driver's tick ran {late:,.0f}s late from +{since:.0f}s, "
                     "as when the host sleeps; nothing was scheduled or polled in that span")
    lines += [f"no {name} phase" for name in ("start", "end_end") if name not in data.phases]
    if lines:
        return Result("run", FAIL, "; ".join(lines), lines)
    return Result("run", PASS, "ran from start through end_end")


def stalled(results):
    """Whether the `run` row in `results` FAILed a stall."""
    return any(result.id == "run" and any(line.startswith(STALL_LINE) for line in result.lines)
               for result in results)


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
    A `tls_reload_failed` line FAILs too, except inside a `rotate` step on its service that
    issued a broken certificate, or, on a service that polls its TLS files, inside any other
    rotate on it with a `tls_reloaded` line of the same component after it in that rotate: a
    poll can read one file before a rotation's rename and the next after it.
    A `token_refresh_failed` warning is counted inside a fault window on an `idp` service and
    WARNs outside every one: it loses nothing, but with the endpoint up it shouldn't happen.
    """
    if not data.stderr:
        return Result("self_log", SKIP, "no logs/*.stderr")
    windows = data.windows()
    status = PASS
    lines = []
    counts = {"error": 0, "fault_key_inside": 0, "fault_key_outside": 0, "text": 0,
              "expected": 0, "tls_expected": 0, "tls_torn": 0, "refresh_inside": 0}
    idp_windows = [window for name, role in data.services.items() if role == IDP_ROLE
                   for window in data.windows(on=name)]
    # `{service: [(start, end)]}` of each `rotate` step issued `broken = true` on it, from its
    # apply to its revert, where a failed TLS reload is expected, and of each other rotate on it.
    broken, whole = {}, {}
    for event in data.events:
        if event.action == "rotate":
            (broken if event.broken else whole).setdefault(event.on, []).append(
                (event.start, event.end))
    polls = {service: tls_reload_polls(_service_config_text(data, service))
             for service in data.stderr}

    def torn_read(service, entry, entries):
        """Whether a failed reload on a polling `service` is a poll that read a set between
        two of a rotation's renames: inside a rotate that wasn't broken, with a reload of the
        same component after it in that rotate. The rename is atomic per file only, and a
        set's files are read one at a time (crates/logit-pipeline/src/tls.rs,
        `Watched::check`)."""
        if entry.ts is None or not polls.get(service):
            return False
        window = next(((start, end) for start, end in whole.get(service, [])
                       if start <= entry.ts <= end), None)
        return window is not None and any(
            later.key == TLS_RELOADED_KEY and later.component == entry.component
            and later.ts is not None and entry.ts < later.ts <= window[1] for later in entries)
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
            if entry.key == TLS_RELOAD_FAILED_KEY:
                if entry.ts is not None and data.in_window(entry.ts, broken.get(service, [])):
                    counts["tls_expected"] += 1
                elif torn_read(service, entry, entries):
                    counts["tls_torn"] += 1
                else:
                    status = FAIL
                    lines.append(f"{service}: {entry.level} {entry.key} at "
                                 f"{data.offset(entry.ts)}, outside every broken rotate on "
                                 f"{service} and with no reload after it in its rotate: "
                                 f"{entry.message[:160]}")
                continue
            if entry.key == TOKEN_REFRESH_FAILED_KEY:
                if entry.ts is not None and data.in_window(entry.ts, idp_windows):
                    counts["refresh_inside"] += 1
                else:
                    status = worst([status, WARN])
                    lines.append(f"{service}: {entry.level} {entry.key} at "
                                 f"{data.offset(entry.ts)}, outside every fault window on an "
                                 f"{IDP_ROLE}: {entry.message[:160]}")
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
               f"line(s), {counts['error']} other ERROR, {counts['text']} non-JSON"
               + (f", {counts['tls_expected']} tls_reload_failed line(s) inside broken "
                  "rotations" if counts["tls_expected"] else "")
               + (f", {counts['tls_torn']} torn read(s) a later reload in their rotation "
                  "replaced" if counts["tls_torn"] else "")
               + (f", {counts['refresh_inside']} {TOKEN_REFRESH_FAILED_KEY} line(s) inside "
                  f"{IDP_ROLE} faults" if counts["refresh_inside"] else ""))
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
    """Every progress window outside fault windows has a delivery from every sink, read from
    the SUT service holding the sinks, each logit's stdout keeps its drain cadence, and each
    sink's newest stored sample stays fresh. Under `[overlap]` a fault that holds one instance
    (`OVERLAP_HOLD_ACTIONS`) opens no window, since the others carry its flows: each window is
    judged on the deliveries summed over the instances no such fault holds in it, and skipped
    when it holds every one."""
    holders = data.instances if data.overlap else [data.sut_out]
    tels = {service: data.telemetry.get(service) for service in holders}
    if any(tel is None for tel in tels.values()) or data.t0 is None:
        return Result("progress", SKIP, "no SUT telemetry or no timeline start")
    window = float(data.thresholds.get("progress_window", 30))
    held = {service: [] for service in holders}
    excluded = data.windows()
    if data.overlap:
        rb = data.recovery_bound
        for fault in data.faults:
            if fault.on in held and fault.action in OVERLAP_HOLD_ACTIONS:
                held[fault.on].append((fault.start, fault.end + rb))
        excluded = [(f.start, f.end + rb) for f in data.faults
                    if not (f.on in held and f.action in OVERLAP_HOLD_ACTIONS)]
    status = PASS
    lines = []
    judged = 0
    unjudged = 0
    judged_after_warmup_s = 0.0
    stalled = {sink: 0 for sink in data.sinks}
    for start, end in data.quiet_intervals(data.t0 + window, excluded):
        t = start
        while t + window <= end:
            up = [service for service in holders
                  if not any(s < t + window and e > t for s, e in held[service])]
            if not up:
                unjudged += 1
                t += window
                continue
            judged += 1
            if t >= data.t0 + data.warmup:
                judged_after_warmup_s += window
            for sink in data.sinks:
                delivered = sum(tels[service].counter_in(
                    "logit.component.batches.delivered", t, t + window, component=sink)
                    for service in up)
                if delivered <= 0:
                    stalled[sink] += 1
                    found = [classify_stall(tels[service], sink, t, t + window)
                             for service in up]
                    verdict = worst([one for one, _ in found])
                    status = worst([status, verdict])
                    why = "; ".join(f"{service}: {text}" if data.overlap else text
                                    for service, (_, text) in zip(up, found))
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
        if (record.get("skipped") or t is None or t < data.t0 + window
                or not data.steady(t, windows=excluded)):
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
    if data.overlap:
        detail = (f"{judged} {window:.0f}s window(s) outside fault windows but an instance's own "
                  f"hold, judged per sink on the instances running through each ({unjudged} "
                  f"with none running skipped){backend}: {'; '.join(per_sink)}; {coverage}")
    else:
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


def _rss_points(data, service, windows):
    """`(by_life, source, sampled)`: `{life: [(ts, bytes)]}` outside `windows` after warmup,
    where they came from, and whether the service had any sample after warmup at all, so a
    service whose every sample sat in a fault window reads as unjudged rather than unsampled."""
    tel = data.telemetry.get(service)
    by_life = {}
    sampled = False
    if tel is not None:
        for ts, value, life in tel.gauge_series("logit.process.memory.resident.bytes"):
            sampled = sampled or data.steady(ts, after_warmup=True, windows=[])
            if data.steady(ts, after_warmup=True, windows=windows):
                by_life.setdefault(life, []).append((ts, value))
    if by_life or sampled:
        return by_life, "telemetry", sampled
    for record in data.stats:
        if record.get("svc") != service:
            continue
        value = parse_mem(record.get("MemUsage"))
        if value is None:
            continue
        sampled = sampled or data.steady(record.get("t"), after_warmup=True, windows=[])
        if data.steady(record.get("t"), after_warmup=True, windows=windows):
            by_life.setdefault(0, []).append((record["t"], value))
    return by_life, "docker stats", sampled


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
    from one; WARN when the judged slopes cover under `COVERAGE_MIN_SHARE` of the run. Steady
    here excludes `rss_windows()`, each fault's window plus `rss_settle`. Each service is
    judged apart and `[expected]` can set its verdict (`judge_services`)."""
    limit = float(data.thresholds.get("rss_growth_mib_per_hour", 64))
    windows = data.rss_windows()
    over = WARN if data.duration < SECONDS_PER_HOUR else FAIL
    judged = {}
    extra = []
    fitted = False
    for service in data.logit_services:
        by_life, source, sampled = _rss_points(data, service, windows)
        if not by_life and not sampled:
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
            judged_s += data.steady_span(points, windows)
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
        fits = (f"MiB/h {', '.join(slopes)}" if slopes
                else "no life with enough samples" if by_life
                else "every sample after warmup in a fault window")
        judged[service] = (status, f"{fits} (limit {limit:g}); {text}")
    expected = (data.resolved.get("expected") or {}).get("rss_slope")
    status, lines = judge_services(data, "rss_slope", judged)
    # With no life fitted, the row SKIPs only when the run after warmup was too short to hold
    # a fit; a longer run whose fault windows left nothing to judge keeps the coverage WARN.
    could_fit = data.post_warmup_s() >= SLOPE_MIN_SPAN_S
    if status == SKIP or not (fitted or expected or could_fit):
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
# One unit runs through the ledger: a line is one event is one increment, and on a UDP wire one
# datagram. A source's `fan_out` node is the exception, and `Ledger.through` carries a count
# across it. The plan's "The ledger and identities" has the symbol table; `Ledger` computes each
# symbol per SUT process life. Each `[[ledger.source]]` is a generator and the SUT listener its
# lines reach over the source's `wire` (`WIRES`): the wire is judged per source, the intake per
# listener, and from the edge into `aggregate` on the listeners' symbols are summed.

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
NO_VM_STANDIN = "standin target: no VictoriaMetrics"


def no_vm_reason(data):
    return NO_VM_KAFKA if data.kafka else NO_VM_STANDIN if data.standin else NO_BACKEND


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
# backend a sink has nothing rejected and a spool reads back nothing corrupt. A stand-in told to
# reject isn't a healthy backend, so a `rejected` drop inside such a step doesn't FAIL
# (`Ledger.cued_rejections`).
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


def component_int(text, component, key):
    """An integer field of one component in a YAML text, such as a listener's
    `receive.max_datagrams`, or None."""
    for block in component_blocks(text, component):
        value = _yaml_int(block, key)
        if value is not None:
            return value
    return None


def component_duration(text, component, key):
    """A duration field of one component in a YAML text, block or flow style, in seconds, or
    None."""
    for block in component_blocks(text, component):
        found = re.search(rf"\b{key}\s*:\s*[\"']?([0-9][0-9a-z.]*)", block)
        if found:
            try:
                return parse_duration(found.group(1))
            except ValueError:
                return None
    return None


# The aggregate window assumed when the SUT config names none.
DEFAULT_AGGREGATE_INTERVAL_S = 10.0
# `logit_out`'s `concurrency` and a disk spool's `checkpoint_interval` when the config names none
# (crates/logit-config, `LogitOut` and `DiskBufferConfig`).
DEFAULT_HOP_WINDOW = 32
DEFAULT_CHECKPOINT_INTERVAL_S = 1.0

# The listener symbols the ledger keeps per listener, each also summed over the listeners.
LISTENER_SYMBOLS = ("W", "K", "D_tel", "D_shutdown", "B", "E", "X", "F")


@dataclass(frozen=True)
class Wire:
    """One wire's ledger symbols (the plan's "The ledger and identities"): `g`, the generator
    sink's counter for G; `w`, the listener's counter for W; `k`, its kernel drops, or None where
    the wire has none; `counted`, the `logit.input.frames.dropped` reasons that lose a line
    before W counts it, F, counted wire loss; and `b_frames`, the `frames.dropped` reasons in B,
    lines W counted and the listener then refused."""

    label: str
    g: str
    w: str
    k: object
    counted: tuple
    b_frames: tuple


# A TCP listener counts `input.frames` once its framer has a whole line, so a line its framer
# drops (`truncated`, `oversize`, `drained`, or `malformed`) never reaches W and is F, and only
# `undecodable`, counted after `input.frames`, is in B (docs/design/internal-telemetry.md,
# "statsd_in"). A UDP listener counts the datagram first, so a `lines_in` line over
# `max_line_bytes` is in W and its `oversize` drop is B. A file wire's G is the writer's
# `file_out`, which counts no `output.messages`. An HTTP wire's G is the writer's points in the
# batches its listener answered with a success, and W the listener's `events.sent`, since
# neither listener counts points before it sends them (`HTTP_LISTENERS`); a request the
# listener refuses decodes to nothing W counts, so the wire has no F and no B.
WIRES = {
    "udp": Wire("UDP", "logit.output.messages", "logit.input.datagrams",
                "logit.input.kernel.dropped", (), ("undecodable", "oversize")),
    "tcp": Wire("TCP", "logit.output.messages", "logit.input.frames", None,
                ("truncated", "oversize", "drained", "malformed"), ("undecodable",)),
    "file": Wire("file", "logit.component.events.delivered", "logit.input.lines", None,
                 ("oversize", "truncated"), ()),
    "http": Wire("HTTP", "logit.component.events.delivered", "logit.component.events.sent",
                 None, (), ()),
}


@dataclass(frozen=True)
class HttpListener:
    """One HTTP listener kind's symbols beside its wire's: `requests`, its per-request counter,
    whose `class` tags `ok`, `busy`, and `closed_consumer` split the answers on its write route
    `route`, or None for a listener that counts no requests; and `deferred`, the batches a busy
    `503` left with the client, or None for a listener that never answers busy."""

    requests: object
    route: object
    deferred: object


# `influxdb_in` decodes a body whole into one batch and sends it under one deadline, answering
# `204` once it is in every inbox, `503` busy with nothing taken, and `503` `closed_consumer` with
# the batch counted sent and its events dropped (crates/logit-inputs/src/influxdb.rs,
# "Responses"; docs/design/internal-telemetry.md, "influxdb_in"). So per life its `ok` and
# `closed_consumer` requests sum to its `batches.sent`, and its `busy` requests equal its
# `batches.deferred`. `otlp_in` counts no requests and never answers busy: a slow downstream
# holds the request, and a client that gives up leaves its batches to be delivered
# (crates/logit-inputs/src/otlp.rs).
HTTP_LISTENERS = {
    "influxdb_in": HttpListener("logit.input.requests", "write", "logit.input.batches.deferred"),
    "otlp_in": HttpListener(None, None, None),
}
# A file wire's tailer reads at most this much of one file per read, so a kill can replay up to
# one read's lines per file past its last checkpoint (docs/known-gaps/tailing.md, "The tail
# driver notices shutdown only between two files' reads").
TAIL_READ_CHUNK_BYTES = 64 * 1024
# `tail_in`'s `checkpoint_interval` when the config names none (crates/logit-config,
# `TailOptions`).
DEFAULT_TAIL_CHECKPOINT_INTERVAL_S = 5.0
# The files a killed life can replay a read's lines of: the writer's active file, and the file
# it rotated away inside the life's last checkpoint interval, since a rotation every few tens of
# seconds leaves at most one inside an interval of a few seconds. A fully read rotated file
# replays nothing.
TAIL_REPLAY_FILES = 2
# `file_out`'s `rotate.max_files` when the config names none (crates/logit-config,
# `RotateConfig`). It counts the active file.
DEFAULT_FILE_OUT_MAX_FILES = 5
# The SUT actions during which a file wire's tailer reads nothing while its writer keeps
# rotating (`_file_retention_problems`).
FILE_RETENTION_ACTIONS = ("pause", "stop", "kill", "restart", "exit")
_SIZE_UNITS = {"B": 1, "KB": 1000, "MB": 1000 ** 2, "GB": 1000 ** 3,
               "KiB": 1024, "MiB": 1024 ** 2, "GiB": 1024 ** 3}
# On a TCP wire the actions that end a SUT or generator process, or a partition that moved an
# address, can lose lines the generator counted as sent: what sat in either side's socket buffers
# when the connection reset. Their windows are reported, not judged. Every other TCP-affecting
# window is judged as steady state is, with a surplus allowed of one generator batch per
# `ambiguous_at_most_once` drop in it: a write that failed partway left whole lines the SUT read
# and the generator never counted.
TCP_REPORTED_ACTIONS = ("stop", "kill", "restart", "exit")
AMBIGUOUS_DROP = "ambiguous_at_most_once"
# On an HTTP wire G counts only points the listener answered with a success, which it does once
# their batch is in every inbox, after W counted it, so no window can lose a point G counted. A
# SIGKILL or an exit loses up to one `internal` interval of the SUT's unexported W, so those
# windows are reported. Every other window is judged with a surplus allowed of one generator
# batch per attempt that ended in an ambiguous transport error (`HTTP_AMBIGUOUS_CLASS`, a
# timeout or a connection reset or closed before the answer): the writer sends one batch per
# request with one request in flight (crates/logit-outputs/src/influxdb.rs, "Response classes";
# crates/logit-outputs/src/otlp.rs) and resends the whole batch after such an attempt, so each
# can leave at most one batch the listener took that G never counts. A connect failure
# (`HTTP_CONNECT_CLASS`: a refused or failed dial, a DNS failure) sent nothing and adds none.
# A writer built before the HTTP sinks split the two counts both as `network_error`, so on its
# run the allowance is an upper bound. A `503` takes nothing or counts what it refused as X
# (`HTTP_LISTENERS`), and adds none.
HTTP_REPORTED_ACTIONS = ("kill", "exit")
HTTP_AMBIGUOUS_CLASS = "network_error"
HTTP_CONNECT_CLASS = "connect_error"


def source_wire(source):
    """A `[[ledger.source]]`'s wire name, `udp` when it names none."""
    return source.get("wire") or DEFAULT_WIRE


def _summed(by_listener):
    """`{life: value}` summed over `{listener: {life: value}}`."""
    total = {}
    for lives in by_listener.values():
        for life, value in lives.items():
            total[life] = total.get(life, 0) + value
    return total


class SutLives:
    """One SUT service's telemetry and process lives, split where its `logit.process.uptime`
    decreases and cross-checked against the starts the timeline gives that service, with the
    lives a scheduled `kill` on it ended. `problems` holds why the ledger can't read it."""

    def __init__(self, data, service):
        self.service = service
        self.tel = data.telemetry.get(service)
        self.problems = []
        self.life_starts, self.lives, self.final = [], [], None
        self.killed_at, self.killed = {}, []
        who = "SUT" if len(data.suts) == 1 else f"SUT {service}"
        if self.tel is None:
            self.problems.append(f"no {who} telemetry")
            return
        if not self.tel.drains:
            self.problems.append(f"{who} telemetry has no logit.process.uptime points")
            return
        self.life_starts = self.tel.life_starts
        self.lives = list(range(len(self.life_starts))) or [0]
        self.final = self.lives[-1]
        # Each start of the service's container begins a life: a stop's or a kill's revert, an
        # end-sequence early one included, and a restart's apply. An unpause doesn't. A life
        # ended by a kill writes no shutdown drain, so its last drain is an ordinary one, and the
        # next life's first uptime is lower. A life under `internal`'s
        # interval leaves at most its shutdown uptime point, so the next life's first uptime isn't
        # lower and the two merge; the ledger isn't judged over merged windows.
        expected = 1 + sum(
            1 for record in data.timeline
            if record.get("on") == service and not record.get("rc") and is_start(record))
        if len(self.lives) != expected:
            self.problems.append(
                f"{who} telemetry shows {len(self.lives)} life/lives where logit.process.uptime "
                f"decreases, but the timeline starts the "
                f"{'SUT' if len(data.suts) == 1 else 'service'} into {expected}")
        # The lives a scheduled `kill` on this service ended, each the life in force when the
        # kill was applied, with the time it was.
        self.killed_at = {
            vm.life_at(self.life_starts, record["started_at"]): record["started_at"]
            for record in data.timeline
            if record.get("event") == "apply" and record.get("action") == "kill"
            and record.get("on") == service and not record.get("rc")}
        self.killed = sorted(self.killed_at)
        # When each of those kills' `docker kill` returned: the signal lands before then.
        self.killed_done = {
            vm.life_at(self.life_starts, record["started_at"]): record.get("finished_at")
            for record in data.timeline
            if record.get("event") == "apply" and record.get("action") == "kill"
            and record.get("on") == service and not record.get("rc")}


class Ledger:
    """The ledger's symbols per SUT process life, read once from a run's telemetry, stderr, and
    VictoriaMetrics exports; each generator's G is one number for the run. The listener symbols
    are read from the SUT service holding the listeners (`ingress`, on `inn`'s lives), kept per
    listener (`per[listener][symbol]`) and summed under their own names, which every row from
    the edge into `aggregate` on reads. Everything from `aggregate` on is read from the SUT
    service holding it and the sinks (`sut`), on its lives (`lives`, `final`, `killed`). With
    one SUT the two are the same. V is per sink: `V[sink]`, each from its own export, split on
    the sinks' service's lives. With two SUT services a `[[ledger.hop]]` joins them (`hop`): the
    ingress SUT's hop sink takes S_hop, the far SUT's `logit_in` sends E_B into `aggregate`, and
    E_B − X_B stands in for the far side's W − D − B − X (`fed`). Lives never cross the hop:
    `ledger.hop` judges it on run totals."""

    def __init__(self, data):
        cfg = data.ledger
        self.data = data
        # Each `[[ledger.source]]`: a generator service and the SUT listener it reaches.
        self.sources = [dict(source) for source in cfg.get("source") or []]
        self.listeners = [source.get("sut_listener") for source in self.sources]
        # `{listener: Wire}`, from each source's `wire`; with any but UDP the symbols carry F.
        self.wires = {source.get("sut_listener"): WIRES[source_wire(source)]
                      for source in self.sources}
        self.streams = any(source_wire(source) != DEFAULT_WIRE for source in self.sources)
        self.aggregate = cfg.get("sut_aggregate")
        self.sinks = list(data.sinks)
        self.split = data.split
        # Under `route` and `target`, the `target` feeding each sink.
        self.targets = {sink["id"]: sink.get("target") for sink in cfg.get("sink") or []}
        # Under `target`, the router filling the targets (`sut_router`), and per sink its
        # branch: the transforms between its target and its own `aggregate`, then that
        # `aggregate`. The listeners' rows end at the router, and each sink's rows start at its
        # target (`router_balance`, `branch_fed`).
        self.targeted = self.split == TARGET_SPLIT
        self.router = cfg.get("sut_router") if self.targeted else None
        self.branches = ({sink["id"]: list(sink.get("transforms") or [])
                          for sink in cfg.get("sink") or []} if self.targeted else {})
        self.aggregates = ({sink["id"]: sink.get("aggregate") for sink in cfg.get("sink") or []}
                           if self.targeted else {})
        # The `lua` component between the listener and `aggregate`, or None.
        self.script = cfg.get("sut_script")
        # How the egress rows write the increments `aggregate` absorbed from the listener's
        # events: with a script, its `flush()` events are absorbed too, and V excludes them.
        self.ab_text = "Ab − L_flush" if self.script else "Ab"
        # How a stopped life's residual and egress gap write the events the path to `aggregate`
        # removed, counted (`discarded`).
        chained = any(source.get("sut_transforms") for source in self.sources)
        # Each source's `fan_out` nodes, `{node: k}`. With one the listeners' symbols are read
        # carried through each chain, W′ (`through`), which counts the chain's discards itself.
        self.fan_out = {}
        for source in self.sources:
            self.fan_out.update(source.get("fan_out") or {})
        self.reshaped = bool(self.fan_out)
        self.discarded_text = ("(L_drop + L_err) − " if self.script
                               else "" if self.reshaped
                               else "ΣT_disc − " if chained else "")
        self.residual_text = ("E_B − X_B − Ab" if cfg.get("hop")
                              else "W′ − R_in" if self.reshaped and self.targeted
                              else f"W − D − B − X − {self.discarded_text}R_in" if self.targeted
                              else "W′ − Ab" if self.reshaped
                              else f"W − D − B − X − {self.discarded_text}"
                              + ("(Ab − L_flush)" if self.script else "Ab"))
        # `{service: SutLives}` for each SUT service, the one holding the listeners, and the one
        # holding `aggregate` and the sinks.
        self.views = {service: SutLives(data, service) for service in data.suts}
        self.inn, self.out = self.views[data.sut_in], self.views[data.sut_out]
        self.service = data.sut_out
        self.across = data.sut_in != data.sut_out
        self.ingress, self.sut = self.inn.tel, self.out.tel
        # The `[[ledger.hop]]` between the two, or None with one SUT.
        hops = cfg.get("hop") or []
        self.hop = dict(hops[0]) if hops else None
        # `{service: telemetry}` for each source's generator that wrote any.
        self.gens = {source.get("service"): data.telemetry[source.get("service")]
                     for source in self.sources if source.get("service") in data.telemetry}
        # Reasons every row that reads the SUT's telemetry SKIPs.
        self.problems = [problem for view in self.views.values() for problem in view.problems]
        if self.across and self.hop is None:
            self.problems.append(f"SUT services {data.sut_in} and {data.sut_out} have no "
                                 "[[ledger.hop]] joining them")
        self.killed = []
        self.http_kinds = {}
        if any(view.tel is None or not view.tel.drains for view in self.views.values()):
            self.lives = []
            return
        self.life_starts, self.lives, self.final = (self.out.life_starts, self.out.lives,
                                                    self.out.final)

        sut = self.ingress
        # `{listener: kind}` for each listener on an HTTP wire, from the SUT config, which
        # `load()` held to a kind `HTTP_LISTENERS` names; one it can't read has none.
        config = self._sut_config_text(self.inn.service)
        self.http_kinds = {}
        for source in self.sources:
            listener = source.get("sut_listener")
            if source_wire(source) == HTTP_WIRE:
                kind = listener_wire(config, listener)[1]
                if kind in HTTP_LISTENERS:
                    self.http_kinds[listener] = kind
        self.per = {}
        for listener in self.listeners:
            one = self.per[listener] = {}
            wire = self.wires[listener]
            one["W"] = sut.counter_by_life(wire.w, component=listener)
            one["K"] = (sut.counter_by_life(wire.k, component=listener) if wire.k else {})
            # D, every reason. Every SUT config gives `internal` a dedicated pipeline, so its
            # final drain runs after every data node has exited and carries the shutdown drops.
            one["D_tel"] = sut.counter_by_life("logit.component.datagrams.dropped",
                                               component=listener)
            one["D_shutdown"] = sut.counter_by_life("logit.component.datagrams.dropped",
                                                    component=listener, reason="shutdown")
            # B: a line the decoder refused (`messages.dropped{reason="malformed"}`), a datagram
            # or frame it refused whole (`frames.dropped{reason="undecodable"}`), and on a UDP
            # wire a line over `lines_in`'s `max_line_bytes`, which its decoder drops without a
            # decoder error (`Wire.b_frames`). Each is 0 on a listener that has no such rule. A
            # generator line is one datagram or frame, so a refused one is one line.
            one["B"] = _summed({
                "malformed": sut.counter_by_life("logit.input.messages.dropped",
                                                 component=listener, reason="malformed"),
                **{reason: sut.counter_by_life("logit.input.frames.dropped",
                                               component=listener, reason=reason)
                   for reason in wire.b_frames}})
            # F: the lines the wire's framer dropped before W counted them, counted wire loss.
            one["F"] = _summed({reason: sut.counter_by_life("logit.input.frames.dropped",
                                                            component=listener, reason=reason)
                                for reason in wire.counted})
            one["E"] = sut.counter_by_life("logit.component.events.sent", component=listener)
            # Events the listener counted as sent and then dropped, every reason: a batch sent
            # into the inbox of a node that has failed is `closed_consumer`.
            one["X"] = sut.counter_by_life("logit.component.events.dropped", component=listener)
            # On an HTTP wire, the listener kind's own symbols (`HTTP_LISTENERS`): its batches
            # sent, its write route's requests by class, and the batches it deferred.
            kind = self.http_kinds.get(listener)
            if kind is not None:
                symbols = HTTP_LISTENERS[kind]
                one["batches"] = sut.counter_by_life("logit.component.batches.sent",
                                                     component=listener)
                for cls in ("ok", "busy", "closed_consumer"):
                    one[f"requests_{cls}"] = (
                        sut.counter_by_life(symbols.requests, component=listener,
                                            route=symbols.route, **{"class": cls})
                        if symbols.requests else {})
                one["deferred"] = (sut.counter_by_life(symbols.deferred, component=listener)
                                   if symbols.deferred else {})
        for symbol in LISTENER_SYMBOLS:
            setattr(self, symbol, _summed({listener: one[symbol]
                                           for listener, one in self.per.items()}))
        sut = self.sut
        if self.hop is not None:
            self._read_hop(data)
        # Batches any data node dropped for shutdown, the sink's and a Lua node's alike, which
        # `drain complete`'s `batches_dropped` cross-checks.
        self.shutdown_batches = sut.counter_by_life("logit.component.batches.dropped",
                                                    reason="shutdown")
        # `{life: {(component, reason): batches}}` under every reason, which `drop_credit` reads.
        # The reason set is open: a reason this file doesn't name still counts a gap. Except
        # `disk_torn`: a spool counts it at open for a batch an earlier life received, so it
        # can't explain a gap in the life that counts it, as `buffer.disk.replayed` can't.
        self.dropped_batches = {}
        for point in sut.matching("logit.component.batches.dropped"):
            if point.attributes.get("reason") == "disk_torn":
                continue
            if isinstance(point.value, (int, float)) and point.value:
                key = (point.attributes.get("component"), point.attributes.get("reason"))
                life = self.dropped_batches.setdefault(point.life, {})
                life[key] = life.get(key, 0) + point.value
        self.A = sut.counter_by_life("logit.component.events.received",
                                     component=self.aggregate)
        self.Ab = sut.counter_by_life("logit.transform.metrics.absorbed",
                                      component=self.aggregate)
        if self.targeted:
            # The router's own counts: what it took, the deliveries past each event's first
            # (`copied`), the events no target took (`discarded`, only `route_signals` counts
            # it), and what it sent on its own forward edge, a `route`'s ordinary consumer. Its
            # targets' sends are each counted under the target's id (T_sent).
            router = self.router
            self.R_in = sut.counter_by_life("logit.component.events.received", component=router)
            self.R_cp = sut.counter_by_life("logit.component.events.copied", component=router)
            self.R_disc = sut.counter_by_life("logit.component.events.discarded",
                                              component=router)
            self.R_fwd = sut.counter_by_life("logit.component.events.sent", component=router)
            # Per sink: its target's sends that a closed consumer dropped, and its own
            # `aggregate`'s A, Ab, and events sent.
            self.T_x = {sink: sut.counter_by_life("logit.component.events.dropped",
                                                  component=self.targets.get(sink))
                        for sink in self.sinks}
            self.A_k = {sink: sut.counter_by_life("logit.component.events.received",
                                                  component=self.aggregates.get(sink))
                        for sink in self.sinks}
            self.Ab_k = {sink: sut.counter_by_life("logit.transform.metrics.absorbed",
                                                   component=self.aggregates.get(sink))
                         for sink in self.sinks}
            self.A_sent_k = {sink: sut.counter_by_life("logit.component.events.sent",
                                                       component=self.aggregates.get(sink))
                             for sink in self.sinks}
        if self.script:
            script = self.script
            self.L_in = sut.counter_by_life("logit.component.events.received", component=script)
            # Counts events: an `emit_many` outcome adds one per event it returns.
            self.L_emit = sut.counter_by_life("logit.script.events.emitted", component=script)
            # A `nil` return is an intended discard; an error loses its one input event, which
            # the runtime counts as loss beside `errors{reason="process"}`.
            self.L_drop = sut.counter_by_life("logit.component.events.discarded",
                                              component=script)
            self.L_err = sut.counter_by_life("logit.component.events.dropped", component=script,
                                             reason="script_error")
            self.L_flush = sut.counter_by_life("logit.component.flush.events", component=script)
            self.L_sent = sut.counter_by_life("logit.component.events.sent", component=script)

        # The lives a scheduled `kill` on the sinks' service ended, each the life in force when
        # the kill was applied, with the time it was.
        self.killed_at, self.killed = self.out.killed_at, self.out.killed

        # The stderr copies of the shutdown counts, each credited to the life of its own service
        # its timestamp falls in, read only to cross-check telemetry (`datagram_mismatch` from
        # the listeners' service, `batch_mismatch` from the sinks'). `drain complete` also counts
        # the `internal` pipeline's own shutdown drops, which no drain can export. A killed life
        # writes neither.
        self.D_log = {}
        self.D_log_per = {listener: {} for listener in self.listeners}
        self.batches_dropped = {}
        self.drain_completes = {}
        for entry in data.stderr.get(self.inn.service, []):
            if entry.kind != "json" or entry.ts is None:
                continue
            life = vm.life_at(self.inn.life_starts, entry.ts)
            match = SHUTDOWN_DROP_RE.match(entry.message)
            if match and life not in self.inn.killed and entry.component in self.D_log_per:
                logged = self.D_log_per[entry.component]
                logged[life] = logged.get(life, 0) + int(match.group(1))
                self.D_log[life] = self.D_log.get(life, 0) + int(match.group(1))
        for entry in data.stderr.get(self.service, []):
            if entry.kind != "json" or entry.ts is None:
                continue
            life = vm.life_at(self.life_starts, entry.ts)
            if entry.message == "drain complete":
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
        self.point_copies = {}
        self.topic_format = data.target.get("format", topic.JSON)
        self.kafka_sink = self.sinks[0] if data.kafka and self.sinks else None
        if self.kafka_sink is not None:
            sink = self.kafka_sink
            read = topic.parse_records(data.topic_text or "", fmt=self.topic_format)
            self.records, self.unparsed = read.records, read.unparsed
            self.V[sink], self.V_total[sink] = topic.totals_by_life(self.records,
                                                                    self.life_starts)
            self.duplicates = topic.duplicates_by_life(read.duplicates, self.life_starts)
            self.point_copies = topic.duplicates_by_life(read.point_copies, self.life_starts)
            self.resets[sink], self.mismatched[sink] = {}, []
        # A `datadog` or `influxdb` stand-in's one sink has the stored points for its V,
        # credited to lives by point timestamp as a Kafka record is: under `datadog` distinct
        # copies only, each extra copy a duplicate the destination counted again, and under
        # `influxdb` one per `(series, timestamp)`, a resend overwriting (`ledger.standin`
        # judges both).
        self.counts_sink = (self.sinks[0] if data.standin_counts is not None and self.sinks
                            else None)
        if self.counts_sink is not None:
            sink = self.counts_sink
            self.V[sink], self.V_total[sink] = topic.totals_by_life(
                data.standin_counts.points, self.life_starts)
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

        # Each source's `sut_transforms`, and every one's events received, sent, and discarded,
        # which `ledger.edge` judges hop by hop. The runtime counts a Transform node's
        # `events.discarded` only when nonzero, so an absent count is 0.
        self.transforms = {source.get("sut_listener"): list(source.get("sut_transforms") or [])
                           for source in self.sources}
        self.T_in, self.T_out, self.T_disc, self.T_lines = {}, {}, {}, {}
        # Under `target`, each sink's branch transforms too, read the same way; `t_disc` sums
        # the sources' chains alone.
        for chain in list(self.transforms.values()) + list(self.branches.values()):
            for component in chain:
                self.T_in[component] = sut.counter_by_life("logit.component.events.received",
                                                           component=component)
                self.T_out[component] = sut.counter_by_life("logit.component.events.sent",
                                                            component=component)
                self.T_disc[component] = sut.counter_by_life(
                    "logit.component.events.discarded", component=component)
                # The lines a `fan_out` node lost, under every reason: only `line_protocol`
                # counts it, under `malformed`.
                self.T_lines[component] = sut.counter_by_life(
                    "logit.transform.lines.dropped", component=component)

        # Per source, from its generator's telemetry: G and its wire's precondition, the sink's
        # counted loss (`drop_newest`, and on a stream `ambiguous_at_most_once`) by reason, and
        # the `generate_in` batch size.
        self.gen_stats = {}
        for source in self.sources:
            service = source.get("service")
            gen = self.gens.get(service)
            if gen is None:
                continue

            def total(name, components, gen=gen):
                return sum(sum(gen.counter_by_life(name, component=component).values())
                           for component in components)

            sinks = source_ids(source, "generator_sink")
            dropped_by = {}
            for sink in sinks:
                for point in gen.matching("logit.component.batches.dropped", component=sink):
                    if isinstance(point.value, (int, float)) and point.value:
                        reason = point.attributes.get("reason", "?")
                        dropped_by[reason] = dropped_by.get(reason, 0) + point.value
            # Each input's batch. A generator with several inputs sends a batch from each in
            # about the same tick, so its batch, the wire's edge tolerance, is their sum; a
            # sink's own batch is its paired input's (`source_pairs()`).
            input_batch = {}
            for load in source_ids(source, "generator_input"):
                sent = total("logit.component.events.sent", [load])
                batches = total("logit.component.batches.sent", [load])
                input_batch[load] = sent / batches if batches else 0.0
            batch = sum(input_batch.values())
            self.gen_stats[service] = {
                "G": total(self.wires[source.get("sut_listener")].g, sinks),
                "dropped_by": dropped_by,
                "G_datagrams": total("logit.output.datagrams", sinks),
                "dropped_events": total("logit.component.events.dropped", sinks),
                "dropped_batches": total("logit.component.batches.dropped", sinks),
                "batch": batch,
                "sink_batch": {sink: input_batch.get(load, 0.0)
                               for load, sink in source_pairs(source)},
                # An HTTP writer's events received, and its requests answered `5xx`, which it
                # retries.
                "attempted": total("logit.component.events.received", sinks),
                "answered_5xx": sum(sum(gen.counter_by_life(
                    "logit.output.requests", component=sink, **{"class": "5xx"}).values())
                    for sink in sinks),
            }
        self.G = sum(stats["G"] for stats in self.gen_stats.values())

        # R: per listener its receive queue and `LISTENER_RESIDUAL_BATCHES`, plus
        # `NODE_RESIDUAL_BATCHES` for each node between it and `aggregate`; and `aggregate`'s own
        # `NODE_RESIDUAL_BATCHES`, plus a script's, in batches of the largest `batch_max_events`.
        text = self._sut_config_text(self.inn.service)
        self.R, parts, largest = 0, [], 0
        for listener in self.listeners:
            # Only a UDP listener has a receive queue; a stream connection's flow control is
            # its backpressure.
            max_datagrams = (0 if self.wires[listener] is not WIRES["udp"]
                             else component_int(text, listener, "max_datagrams")
                             or DEFAULT_MAX_DATAGRAMS)
            batch_max = (component_int(text, listener, "batch_max_events")
                         or DEFAULT_BATCH_MAX_EVENTS)
            largest = max(largest, batch_max)
            batches = (LISTENER_RESIDUAL_BATCHES
                       + NODE_RESIDUAL_BATCHES * len(self.transforms.get(listener, [])))
            # A residual counted in W′ is worth up to the chain's fan-out per event it holds.
            factor = self.factor(listener)
            self.R += (max_datagrams + batches * batch_max) * factor
            part = f"{_n(max_datagrams)} + {batches} x {_n(batch_max)}"
            parts.append((f"({part}) x {factor}" if factor > 1 else part)
                         + (f" ({listener})" if len(self.listeners) > 1 else ""))
        shared = NODE_RESIDUAL_BATCHES * (2 if self.script else 1)
        widest = max([self.factor(listener) for listener in self.listeners] or [1])
        self.R += shared * largest * widest
        parts.append(f"{shared} x {_n(largest)}" + (f" x {widest}" if widest > 1 else ""))
        self.R_text = f"R {_n(self.R)} = " + " + ".join(parts)
        if self.reshaped:
            self.R_text += ("; W′ is each listener's W − D − B − X carried through its chain's "
                            "fan_out and discards")
        if self.hop is not None:
            # Across a hop, the far SUT's residual is what `aggregate`'s inbox and its taken
            # batch can hold, in frames of at most `per_frame` events; the ingress SUT's R
            # covers its hop sink's inbox as it would `aggregate`'s.
            self.R_hop = round(NODE_RESIDUAL_BATCHES * self.per_frame)
            self.R_hop_text = (f"R_hop {_n(self.R_hop)} = {NODE_RESIDUAL_BATCHES} x "
                               f"{self.per_frame:,.0f} events per frame")
        # The aggregate's configured window, in seconds, or None when the config doesn't say;
        # under `target` each sink's own (`intervals`), and the longest of them.
        config = self._sut_config_text(self.service)
        self.interval = component_duration(config, self.aggregate or "", "interval")
        self.intervals = {sink: component_duration(config, aggregate or "", "interval")
                          for sink, aggregate in self.aggregates.items()}
        if self.targeted:
            self.interval = max([one for one in self.intervals.values() if one] or [None],
                                key=lambda one: one or 0)
        # Each `respond` step that expects the `rejected` class, as `(start, end, step)`: its
        # span widened by one aggregate window and `recovery_bound`, inside which a sink's
        # `rejected` drop is the step's doing (`cued_rejections`). `ledger.sent` excuses in the
        # same windows the records a step explains, a `413`'s `oversize` and any other rejected
        # status's or a `series_errors` answer's `rejected` (`cued_records`), and the refusal
        # lines of a step that expects `refused` (`cued_refusals`).
        widen = (self.interval or DEFAULT_AGGREGATE_INTERVAL_S) + data.recovery_bound

        def cued(match):
            return [(fault.start, fault.end + widen, fault.step) for fault in data.faults
                    if fault.action == "respond" and match(fault)]

        self.cued = cued(lambda fault: fault.fault_class == "rejected")
        self.cued_records = {
            "rejected": cued(lambda fault: (fault.fault_class == "rejected"
                                            and fault.status != 413)
                             or fault.series_errors is not None),
            "oversize": cued(lambda fault: fault.fault_class == "rejected"
                             and fault.status == 413),
        }
        self.cued_refusals = cued(lambda fault: fault.fault_class == "refused")

    def _read_hop(self, data):
        """The hop's symbols: on the ingress SUT's lives, its hop sink's S_hop
        (`events.received`), its counted drops (`events.dropped`, every reason), batches, and
        `batches.resumed`; on the far SUT's lives, its `logit_in`'s E_B (`events.sent`, frames it
        forwarded), X_B (`events.dropped`), and `batches.resends` (frames it acknowledged under
        a mark and didn't forward); and the events per frame and the send window that bound a
        resend's surplus."""
        ingress, sut = self.ingress, self.sut
        sink, listener = self.hop.get("sink"), self.hop.get("listener")
        self.S_hop = ingress.counter_by_life("logit.component.events.received", component=sink)
        self.hop_dropped = ingress.counter_by_life("logit.component.events.dropped",
                                                   component=sink)
        self.hop_batches = ingress.counter_by_life("logit.component.batches.received",
                                                   component=sink)
        self.resumed = ingress.counter_by_life("logit.output.batches.resumed", component=sink)
        self.reconnects = ingress.counter_by_life("logit.output.reconnects", component=sink)
        self.E_B = sut.counter_by_life("logit.component.events.sent", component=listener)
        self.X_B = sut.counter_by_life("logit.component.events.dropped", component=listener)
        self.resends = sut.counter_by_life("logit.input.batches.resends", component=listener)
        # The most events one frame carried: a drain's events over its batches, at its peak.
        events = {ts: v for ts, v, _ in ingress.counter_points(
            "logit.component.events.received", component=sink)}
        batches = {ts: v for ts, v, _ in ingress.counter_points(
            "logit.component.batches.received", component=sink)}
        self.per_frame = max((events.get(ts, 0) / n for ts, n in batches.items() if n),
                             default=0.0)
        text = self._sut_config_text(self.inn.service)
        self.window = component_int(text, sink, "concurrency") or DEFAULT_HOP_WINDOW
        self.hop_checkpoint = (component_duration(text, sink, "checkpoint_interval")
                               or DEFAULT_CHECKPOINT_INTERVAL_S)

    def fed(self, life):
        """What reached `aggregate`'s side of the sinks' SUT in `life`: W − D − B − X, or across
        a hop E_B − X_B. With a `fan_out` it's W′, each listener's W − D − B − X carried through
        its chain (`through`)."""
        if self.hop is not None:
            return self.get("E_B", life) - self.get("X_B", life)
        if self.reshaped:
            return sum(self.through(listener, self.lget(listener, "W", life)
                                    - self.lget(listener, "D_tel", life)
                                    - self.lget(listener, "B", life)
                                    - self.lget(listener, "X", life), [life])
                       for listener in self.listeners)
        return self.get("W", life) - self.D(life) - self.get("B", life) - self.get("X", life)

    def factor(self, listener):
        """The events one of `listener`'s events can become by `aggregate`: the product of its
        chain's `fan_out`s, 1 with none."""
        product = 1
        for component in self.transforms.get(listener) or []:
            product *= self.fan_out.get(component, 1)
        return product

    def through(self, listener, events, lives):
        """`events` of `listener` carried through its chain over `lives`: a `fan_out` node of k
        makes each event k, less the lines it dropped, plus the parents it sent on rather than
        discarded (T_in − T_disc), ADR `transform-fan-out`'s count of its output; any other node
        takes off what it discarded."""
        for component in self.transforms.get(listener) or []:
            def total(symbol, component=component):
                return sum(getattr(self, symbol)[component].get(life, 0) for life in lives)
            k = self.fan_out.get(component)
            if k:
                events = k * events - total("T_lines") + total("T_in") - total("T_disc")
            else:
                events -= total("T_disc")
        return events

    def ingress_residual(self, life):
        """Across a hop, what a life of the ingress SUT read but neither sent to its hop sink
        nor dropped, counted: W − D − B − X − S_hop."""
        return (self.get("W", life) - self.D(life) - self.get("B", life) - self.get("X", life)
                - self.get("S_hop", life))

    def _sut_config_text(self, service):
        """SUT `service`'s config: the run directory's copy, else the path `compose.env`
        names. An overlap instance with no `[configs]` key of its own runs `sut`'s."""
        own = service != SUT_SERVICE and (
            service in (self.data.resolved.get("configs") or {})
            or service not in self.data.instances)
        key = service if own else SUT_CONFIG_KEY
        variable = config_env(service) if own else SUT_CONFIG_ENV
        name = (self.data.resolved.get("configs") or {}).get(key)
        candidates = [self.data.run_dir / "configs" / name] if name else []
        try:
            for line in (self.data.run_dir / "compose.env").read_text().splitlines():
                if line.startswith(f"{variable}="):
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
        `route` all of them, whose V add up to what `aggregate` absorbed. The sum is sound because
        `load()` refuses a router that can deliver one event to several sinks there. Under
        `target` each sink alone, against its own `aggregate`."""
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
        if self.hop is not None:
            return (f"{self.service}: E_B {_n(self.get('E_B', life))}, X_B "
                    f"{_n(self.get('X_B', life))}, resends {_n(self.get('resends', life))}, "
                    f"A {_n(self.get('A', life))}, Ab {_n(self.get('Ab', life))}, "
                    + ", ".join(f"V[{sink}] {_n(self.v(life, [sink]))}" for sink in self.sinks))
        if self.targeted:
            text = f"{self.listener_symbols(life)}, {self._router_text(life)}"
            if self.t_disc(life):
                text = f"{text}, ΣT_disc {_n(self.t_disc(life))}"
            return text + "; " + "; ".join(
                f"{sink}: T_sent {_n(self.T_sent.get(sink, {}).get(life, 0))}, T_x "
                f"{_n(self.T_x[sink].get(life, 0))}, A {_n(self.A_k[sink].get(life, 0))}, Ab "
                f"{_n(self.Ab_k[sink].get(life, 0))}, V {_n(self.v(life, [sink]))}"
                for sink in self.sinks)
        script = ""
        if self.script:
            script = "".join(f"{symbol} {_n(self.get(symbol, life))}, " for symbol in (
                "L_in", "L_emit", "L_drop", "L_err", "L_flush", "L_sent"))
        elif self.T_disc:
            script = f"ΣT_disc {_n(self.t_disc(life))}, "
        text = (f"W {_n(self.get('W', life))}, K {_n(self.get('K', life))}, {self._f_text(life)}"
                f"D {_n(self.D(life))} "
                f"({_n(self.get('D_shutdown', life))} at shutdown; stderr says "
                f"{_n(self.get('D_log', life))}), B {_n(self.get('B', life))}, "
                f"E {_n(self.get('E', life))}, X {_n(self.get('X', life))}, {script}"
                f"A {_n(self.get('A', life))}, Ab {_n(self.get('Ab', life))}, "
                + ", ".join(f"V[{sink}] {_n(self.v(life, [sink]))}" for sink in self.sinks))
        if len(self.listeners) > 1:
            text += "; " + "; ".join(
                f"{listener}: " + ", ".join(f"{name} {_n(self.lget(listener, symbol, life))}"
                                            for name, symbol in self._per_listener_symbols())
                for listener in self.listeners)
        return text

    def _f_text(self, life):
        """F as text for a symbol line, "" when every source is UDP, whose wire has none."""
        return f"F {_n(self.get('F', life))}, " if self.streams else ""

    def _per_listener_symbols(self):
        names = [("W", "W"), ("K", "K"), ("D", "D_tel"), ("B", "B"), ("E", "E"), ("X", "X")]
        return names[:2] + [("F", "F")] + names[2:] if self.streams else names

    def listener_symbols(self, life):
        """The listener symbols in a life of the listeners' SUT service, as text."""
        text = (f"W {_n(self.get('W', life))}, K {_n(self.get('K', life))}, {self._f_text(life)}"
                f"D {_n(self.D(life))} "
                f"({_n(self.get('D_shutdown', life))} at shutdown; stderr says "
                f"{_n(self.get('D_log', life))}), B {_n(self.get('B', life))}, "
                f"E {_n(self.get('E', life))}, X {_n(self.get('X', life))}")
        if self.hop is not None:
            text += (f", S_hop {_n(self.get('S_hop', life))}, hop dropped "
                     f"{_n(self.get('hop_dropped', life))}, resumed "
                     f"{_n(self.get('resumed', life))}")
        if len(self.listeners) > 1:
            text += "; " + "; ".join(
                f"{listener}: " + ", ".join(f"{name} {_n(self.lget(listener, symbol, life))}"
                                            for name, symbol in self._per_listener_symbols())
                for listener in self.listeners)
        return text

    def absorbed(self, life, sinks=None):
        """The increments `aggregate` absorbed from the listener's events: Ab, less a script's
        `flush()` events, each of which carries one metric whose series V excludes. Under
        `target`, the sum of the `aggregate`s of `sinks`, every sink when None."""
        if self.targeted:
            return sum(self.Ab_k[sink].get(life, 0) for sink in sinks or self.sinks)
        return self.get("Ab", life) - (self.get("L_flush", life) if self.script else 0)

    def t_disc(self, life):
        """ΣT_disc: the events every `sut_transforms` node discarded in `life`, a sink's branch
        transforms under `target` left out."""
        return sum(self.T_disc[component].get(life, 0) for chain in self.transforms.values()
                   for component in chain)

    def branch_disc(self, sink, life):
        """Under `target`, the events `sink`'s branch transforms discarded in `life`."""
        return sum(self.T_disc[component].get(life, 0) for component in self.branches[sink])

    def branch_fed(self, sink, life):
        """Under `target`, what `sink`'s target handed its branch in `life`, less what the
        branch discarded, counted: T_sent − T_x − ΣT_disc over its transforms. A closed
        consumer's drop (T_x) is counted after T_sent counted the send."""
        return (self.T_sent.get(sink, {}).get(life, 0) - self.T_x[sink].get(life, 0)
                - self.branch_disc(sink, life))

    def branch_text(self, sink):
        """`branch_fed`'s formula for `sink`, as text."""
        target = self.targets.get(sink)
        disc = " − ΣT_disc" if self.branches.get(sink) else ""
        return f"T_sent[{target}] − T_x{disc}"

    def router_balance(self, life):
        """Under `target`, `(out, sent)` for the router in `life`: R_in + R_copied − R_disc, what
        it should have handed on, and R_fwd + ΣT_sent, what its forward edge and its targets
        counted sent. `load()` holds the sinks' targets to the router's, every one, and a
        target's T_sent counts every router that fills it, so the router must be the only
        one that fills them."""
        out = (self.get("R_in", life) + self.get("R_cp", life) - self.get("R_disc", life))
        sent = self.get("R_fwd", life) + sum(self.T_sent.get(sink, {}).get(life, 0)
                                             for sink in self.sinks)
        return out, sent

    def router_bounds(self, life):
        """Under `target`, `[(text, ok)]` for the bounds the router's pooled balance can't
        judge: each target's T_sent ≤ R_in − R_disc, and R_fwd ≤ R_in. A target receives at most
        one delivery of each event the router kept, since a `route` lists a target once per
        value (graph rule 51) and a `route_signals` sends one part per target. `route_batch`
        counts R_in and R_disc before a target's `Fanout` counts the send, so a drain can't
        split them the wrong way. The bounds catch a target receiving more than the router took
        in; a shift between targets below that passes."""
        kept = self.get("R_in", life) - self.get("R_disc", life)
        bounds = [(f"T_sent[{self.targets.get(sink)}] "
                   f"{_n(self.T_sent.get(sink, {}).get(life, 0))} <= R_in − R_disc {_n(kept)}",
                   self.T_sent.get(sink, {}).get(life, 0) <= kept) for sink in self.sinks]
        bounds.append((f"R_fwd {_n(self.get('R_fwd', life))} <= R_in "
                       f"{_n(self.get('R_in', life))}",
                       self.get("R_fwd", life) <= self.get("R_in", life)))
        return bounds

    def _router_text(self, life):
        out, sent = self.router_balance(life)
        return (f"R_in {_n(self.get('R_in', life))}, R_copied {_n(self.get('R_cp', life))}, "
                f"R_disc {_n(self.get('R_disc', life))}, R_fwd {_n(self.get('R_fwd', life))}, "
                f"ΣT_sent {_n(sent - self.get('R_fwd', life))}")

    def discarded(self, life):
        """The events the path to `aggregate` removed, counted, in `life`: a script's discards
        and errors (L_drop + L_err), or the `sut_transforms` nodes' discards (ΣT_disc). A scenario
        has one or the other, so neither counts twice."""
        script = self.get("L_drop", life) + self.get("L_err", life) if self.script else 0
        # With a `fan_out`, W′ has taken the chain's discards in already (`through`), and a
        # fan-out node's discarded parents went on as its children.
        return script + (0 if self.reshaped else self.t_disc(life))

    def residual(self, life, sinks=None):
        """What a life read but neither absorbed nor removed, counted, by its last drain:
        W − D − B − X − Ab, with a script W − D − B − X − (L_drop + L_err) − (Ab − L_flush), with
        `sut_transforms` W − D − B − X − ΣT_disc − Ab, with a `fan_out` W′ − Ab, and across a hop
        E_B − X_B − Ab.

        Under `target` the path splits at the router, so the residual has two kinds. With no
        `sinks`, the listeners' side: what reached the router's side short of the router,
        W − D − B − X (less ΣT_disc) − R_in. With `sinks`, each sink's branch: what its target
        sent that its `aggregate` hadn't absorbed, `branch_fed` − Ab, summed."""
        if self.targeted:
            if sinks is None:
                return self.fed(life) - self.discarded(life) - self.get("R_in", life)
            return sum(self.branch_fed(sink, life) - self.Ab_k[sink].get(life, 0)
                       for sink in sinks)
        return self.fed(life) - self.discarded(life) - self.absorbed(life)

    def kill_band(self, life, sinks=None):
        """`(low, high, text)`: the Ab − V a killed life may show with nothing lost, from its
        ingest rate over its last drain interval, each listener's times its chain's fan-out.

        Below: the residual at the last drain, which leaves out what the path discarded,
        counted, and the reads after it, up to one drain interval, are in no Ab, and an
        `aggregate` flush before the kill can carry them into V. Above: the kill discards
        `aggregate`'s unflushed window, whose events Ab counts and V never holds, up to one
        aggregate interval of ingest, plus one drain interval of queue growth after the last
        drain. The residual itself is reported beside the band and never judged with
        it: `ledger.intake` bounds it.

        Under `target`, the band of `sinks`' branches: the ingest is what their targets sent,
        the residual each branch's, and the window their `aggregate`s' longest."""
        drains = self.sut.life_drains(life)
        gaps = [b - a for a, b in zip(drains, drains[1:])]
        interval = statistics.median(gaps) if gaps else 0.0
        if self.targeted and sinks is not None:
            fed = [self.sut.counter_points("logit.component.events.sent",
                                           component=self.targets.get(sink)) for sink in sinks]
        elif self.hop is not None:
            fed = [self.sut.counter_points("logit.component.events.sent",
                                           component=self.hop.get("listener"))]
        else:
            fed = [[(ts, v * self.factor(listener), point_life) for ts, v, point_life
                    in self.ingress.counter_points(self.wires[listener].w, component=listener)]
                   for listener in self.listeners]
        last_w = sum(v for points in fed for ts, v, point_life in points
                     if point_life == life and drains and ts == drains[-1])
        rate = last_w / gaps[-1] if gaps and gaps[-1] > 0 else 0.0
        window = self.interval
        if self.targeted and sinks is not None:
            window = max([self.intervals.get(sink) or 0 for sink in sinks]) or None
        source = "configured"
        if window is None:
            spaced = next((one for one in self.series.values() if one), [])
            window = vm.sample_spacing_s(spaced) or 0.0
            source = "the export's sample spacing, the config naming none"
        residual = max(0, self.residual(life, sinks))
        low = -(residual + round(rate * interval))
        high = round(rate * window) + round(rate * interval)
        text = (f"band [{_n(low)}, {_n(high)}]: ingest {rate:,.0f}/s over the last drain "
                f"interval; below, the residual {_n(residual)} plus one {interval:g}s drain "
                f"interval of ingest, absorbed after the last drain and flushed before the "
                f"kill; above, one {window:g}s aggregate window ({source}) unflushed at the "
                f"kill plus one drain interval of queue growth after the last drain")
        return low, high, text

    def replay_band(self, listeners):
        """`(low, high, lines)`: the range a file wire's W − G may take over the run, summed per
        ingress SUT life, for the tailers `listeners`, with one line per life that widens it.

        A killed life replays the lines its tailers read after their last checkpoint and loses
        the counts of those it read after its last drain: at most rate × `checkpoint_interval`
        plus one 64 KiB read's lines per file it can replay (`TAIL_REPLAY_FILES`) above, and
        rate × its drain interval below, the rate being its lines over its last drain
        interval. A killed life with one drain has no drain interval, and its last checkpoint can
        be the previous life's, so its allowance above is that drain's W, all of which can
        replay, plus the same reads; below, that drain's W stands in for one drain interval of
        reads a checkpoint after the drain committed and the kill never exported. A life a
        stop, restart, or `exit` ended emits each file's unterminated line at shutdown and the
        rest of it after the restart, one line more per file it had open
        (`logit.input.files.open` at its last drain), above (docs/known-gaps/tailing.md's
        split-line entry). The final life adds nothing."""
        inn = self.inn
        text = self._sut_config_text(inn.service)
        counted = self.data.phases.get("shared_counted") or {}
        line_bytes = (counted.get("bytes", 0) / counted["lines"]
                      if counted.get("lines") else 0.0)
        chunk_lines = round(TAIL_READ_CHUNK_BYTES / line_bytes) if line_bytes else 0
        low = high = 0
        lines = []
        for life in inn.lives:
            if life == inn.final:
                continue
            if life in inn.killed:
                drains = self.ingress.life_drains(life)
                gaps = [b - a for a, b in zip(drains, drains[1:])]
                interval = statistics.median(gaps) if gaps else 0.0
                last = sum(v for listener in listeners
                           for ts, v, point_life in self.ingress.counter_points(
                               self.wires[listener].w, component=listener)
                           if point_life == life and drains and ts == drains[-1])
                rate = last / gaps[-1] if gaps and gaps[-1] > 0 else 0.0
                checkpoint = max((component_duration(text, listener, "checkpoint_interval")
                                  or DEFAULT_TAIL_CHECKPOINT_INTERVAL_S)
                                 for listener in listeners)
                chunks = chunk_lines * TAIL_REPLAY_FILES * len(listeners)
                if len(drains) == 1:
                    # No drain interval to take a rate from, and every line the life read can
                    # be after its last checkpoint, so all of its one drain's W can replay. A
                    # checkpoint between the drain and the kill can instead commit reads the
                    # kill never exported; the drain's W, about one drain interval of reading,
                    # bounds those below.
                    below, above = last, last + chunks
                    lines.append(f"life {life} (killed): [-{_n(below)}, +{_n(above)}]: one "
                                 f"drain, so its W of {_n(last)} lines can all replay, plus "
                                 f"{TAIL_REPLAY_FILES} file(s) x one 64 KiB read of "
                                 f"{_n(chunk_lines)} lines above, and its W below")
                else:
                    below = round(rate * interval)
                    above = round(rate * checkpoint) + chunks
                    lines.append(f"life {life} (killed): [-{_n(below)}, +{_n(above)}]: "
                                 f"{rate:,.0f} lines/s x a {interval:g}s drain interval below; "
                                 f"x a {checkpoint:g}s checkpoint_interval plus "
                                 f"{TAIL_REPLAY_FILES} file(s) x one 64 KiB read of "
                                 f"{_n(chunk_lines)} lines above")
                low -= below
                high += above
            else:
                files = 0
                for listener in listeners:
                    series = [v for ts, v, point_life in self.ingress.gauge_series(
                        "logit.input.files.open", component=listener) if point_life == life]
                    files += int(series[-1]) if series else 1
                high += files
                lines.append(f"life {life} (stopped): [0, +{files}]: one split line per file "
                             f"open at its last drain")
        return low, high, lines

    def egress(self, life, sinks):
        """`(status, verdict, gap)` for one life's egress to `sinks`, whose V is summed (one
        sink under `copy`, all under `route`): `ok`, `counted`, `within band`, `surplus`, or
        `uncounted`.

        A positive gap is counted when the life's telemetry has a batch dropped, for any reason,
        at a node that can explain it (`drop_credit`). Whatever the gap, a `FAULT_DROP_REASONS`
        drop in that credit FAILs the life (`fault_drops`), its verdict unchanged.

        The final life's gap is Ab − V, and with a script Ab − L_flush − V, here and below. A
        stopped earlier life's is W − D − B − X − V, less what the path to `aggregate` removed,
        counted (`discarded`), the residual plus Ab − V, because `aggregate`'s close-time flush
        delivers what that life read but hadn't absorbed by its last ordinary drain into V; the
        final drain runs after that flush, so it exports the shutdown drops and the script's or
        the transforms' discards. A
        killed life has no close-time window and no shutdown drops: its gap is Ab − V alone,
        judged inside `kill_band`; below the band it is a surplus, above it uncounted loss. Its
        residual is what the kill lost from the receive queue and inbox, by design, and is
        reported, not judged here.

        Under `target`, `sinks` is one sink and every term is its branch's: Ab is its own
        `aggregate`'s, and a stopped earlier life's gap starts at its target, T_sent − T_x
        (less its branch transforms' ΣT_disc) − V, the branch residual plus Ab − V. The
        listeners' side up to the router is `ledger.intake`'s, and the router's balance
        `ledger.edge`'s, each in every life a kill didn't end, so the three meet with nothing
        between them unjudged.

        On a Kafka target, V counts each distinct record once, so a negative gap in a life that
        wasn't killed is a `surplus`, never a duplicate: increments V holds and Ab doesn't. A
        `datadog` stand-in's V counts each distinct point copy once, and an `influxdb`
        stand-in's each `(series, timestamp)` once, and both are judged the same way. A
        `datadog` sink's `records.dropped` also counts toward a positive gap, because a request
        a series `2xx` body partly rejected loses series with no batch dropped."""
        status, verdict, gap = self._egress(life, sinks)
        if self.fault_drops(life, sinks):
            status = FAIL
        return status, verdict, gap

    def _egress(self, life, sinks):
        gap = self.absorbed(life, sinks) - self.v(life, sinks)
        if life in self.killed and life != self.final:
            low, high, _ = self.kill_band(life, sinks)
            if gap == 0:
                return PASS, "ok", gap
            if low <= gap <= high:
                return PASS, "within band", gap
            return FAIL, ("surplus" if gap < low else "uncounted"), gap
        if life != self.final:
            gap += self.residual(life, sinks)
        if gap == 0:
            return PASS, "ok", gap
        if gap > 0 and (sum(self.drop_credit(life, sinks).values()) > 0
                        or self.record_drops(life, sinks)):
            return PASS, "counted", gap
        if gap < 0 and (self.data.kafka or self.counts_sink in sinks):
            return FAIL, "surplus", gap
        return FAIL, "uncounted", gap

    def _drops(self, life, keep):
        """`{reason: batches}` dropped in `life` at the components `keep` accepts."""
        drops = {}
        for (component, reason), batches in self.dropped_batches.get(life, {}).items():
            if keep(component):
                drops[reason] = drops.get(reason, 0) + batches
        return drops

    def record_drops(self, life, sinks):
        """`{reason: records}` a `datadog` or `influxdb` stand-in's sink among `sinks` dropped
        in `life` (`logit.output.records.dropped`, which `influxdb_out` never counts), or {}
        for any other sink."""
        if self.counts_sink not in sinks:
            return {}
        drops = {}
        for point in self.sut.matching("logit.output.records.dropped",
                                       component=self.counts_sink):
            if point.life == life and isinstance(point.value, (int, float)) and point.value:
                reason = point.attributes.get("reason", "?")
                drops[reason] = drops.get(reason, 0) + point.value
        return drops

    def _others(self, sinks):
        """The sinks outside `sinks`, and under `target` their branches' nodes too."""
        others = set(self.sinks) - set(sinks)
        for sink in list(others) if self.targeted else ():
            others |= set(self.branches.get(sink) or []) | {self.aggregates.get(sink)}
        return others

    def other_sinks_drops(self, life, sinks):
        """`{reason: batches}` the sinks outside `sinks` dropped in `life`, with their
        branches' nodes under `target`."""
        others = self._others(sinks)
        return self._drops(life, lambda component: component in others)

    def drop_credit(self, life, sinks):
        """`{reason: batches}` dropped in `life` that can explain a gap at `sinks`: every node's,
        less those of the sinks outside the group. Under `copy` another sink's drop loses
        nothing this sink was sent, while a drop upstream of the fan-out (the script,
        `aggregate`) or at this sink can; under `route`, and with one sink, the group holds
        every sink and the count is the life's whole. Under `target` another sink's branch
        (its transforms and `aggregate`) is left out with it, since its drop loses nothing this
        sink's target sent."""
        others = self._others(sinks)
        return self._drops(life, lambda component: component not in others)

    def cued_rejections(self, life, sinks):
        """`({step: batches}, outside)`: the `rejected` batch drops at `sinks` in `life` whose
        drain falls inside a cued window (`self.cued`), by step, and the count outside every
        one. A drain's delta covers the interval before it, so a drop counted in the first
        drain inside a window could predate the step by up to one `internal` interval."""
        return self.cued_split("logit.component.batches.dropped", sinks, "rejected",
                               self.cued, life)

    def cued_split(self, name, sinks, reason, windows, life=None):
        """`({step: count}, outside)`: the `name` counts of `reason` at `sinks`, in `life` or
        every life when None, whose drain falls inside one of `windows` (`(start, end, step)`),
        by step, and the count outside every one."""
        excused, outside = {}, 0
        for sink in sinks:
            for ts, value, point_life in self.sut.counter_points(name, component=sink,
                                                                 reason=reason):
                if (life is not None and point_life != life) or not value:
                    continue
                step = next((step for start, end, step in windows if start < ts <= end), None)
                if step is None:
                    outside += value
                else:
                    excused[step] = excused.get(step, 0) + value
        return excused, outside

    def cued_text(self, life, sinks):
        """The cued `rejected` drops in `life` at `sinks`, as text, or ""."""
        excused, _ = self.cued_rejections(life, sinks)
        if not excused:
            return ""
        order = [step for _, _, step in sorted(self.cued)]
        return "rejected " + ", ".join(f"{_n(excused[step])} cued by {step}"
                                       for step in order if step in excused)

    def fault_drops(self, life, sinks):
        """The `FAULT_DROP_REASONS` drops in `life`'s `drop_credit`, as text, or "", less the
        `rejected` drops a cued step explains (`cued_rejections`)."""
        credit = dict(self.drop_credit(life, sinks))
        excused, _ = self.cued_rejections(life, sinks)
        if credit.get("rejected"):
            credit["rejected"] -= sum(excused.values())
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


# ---- a rolling overlap ---------------------------------------------------------------------------
#
# Under `[overlap]` every instance runs the whole pipeline on one shared UDP port (the plan's
# "Rolling overlap (W12b)"). A row about a process life (`ledger.intake`, `ledger.edge`,
# `ledger.script`, `ledger.aggregate`, `ledger.egress`, `ledger.windows`, `ledger.replay`,
# `identity.sink`, and `drops`, and `ledger.summary`'s per-life terms) judges each instance on
# its own lives through `instance_view()`, V being the series whose `label` names the instance,
# and takes the worst status. The generators' flows move between the instances, so `ledger.wire`, `progress`,
# `recovery`, `ledger.summary`'s bound end to end, and `ledger.overlap` judge them together.

# The actions that hold an instance's port: while one holds it, the other instances carry its
# flows, so `progress` judges only the instances not held.
OVERLAP_HOLD_ACTIONS = ("stop", "kill", "pause", "restart", "exit")
# The instance faults that close its socket at a clean shutdown, whose final sample of
# `receive_buffer.used.bytes` bounds what the close discarded, and those that end the process
# with no final sample or drain, after which `ledger.overlap` can't be judged.
OVERLAP_CLOSES = ("stop", "restart")
OVERLAP_UNJUDGED = ("kill", "exit")
# The smallest `skb->truesize` the kernel charges one small datagram, so a queue charged `used`
# bytes holds at most `used / TRUESIZE_FLOOR_B` of them.
TRUESIZE_FLOOR_B = 512
USED_BYTES = "logit.input.receive_buffer.used.bytes"


def _overlapped(data):
    """Whether `data` is a whole overlap run, which a per-life row splits by instance."""
    return bool(data.overlap) and data.overlap_instance is None


def instance_view(data, instance):
    """`data` as one overlap instance's run: that service alone as the SUT, and each sink's
    series only those whose `label` is the instance's name. Cached on `data`."""
    views = data.__dict__.setdefault("_views", {})
    if instance not in views:
        view = copy.copy(data)
        view.__dict__.pop("_ledger", None)
        view.__dict__.pop("_views", None)
        view.overlap_instance = instance
        view.suts = [instance]
        view.sut_in = view.sut_out = instance
        label = data.overlap.get("label")
        view.vm_series = {sink: [one for one in series
                                 if (one.get("metric") or {}).get(label) == instance]
                          for sink, series in data.vm_series.items()}
        views[instance] = view
    return views[instance]


def _instance_ledgers(data):
    """`{instance: Ledger}`, each on its own `instance_view()`, in `[overlap]` order."""
    return {instance: _ledger(instance_view(data, instance)) for instance in data.instances}


def _per_instance(data, check):
    """`check` run on each instance's view: one detail per instance, the worst status, and
    SKIP only when every instance SKIPs."""
    results = {instance: check(instance_view(data, instance)) for instance in data.instances}
    statuses = [result.status for result in results.values()]
    judged = [status for status in statuses if status != SKIP]
    first = next(iter(results.values()))
    return Result(first.id, worst(judged) if judged else SKIP,
                  " | ".join(f"{instance}: {result.detail}"
                             for instance, result in results.items()),
                  [f"{instance}: {line}" for instance, result in results.items()
                   for line in result.lines])


def _unlabeled(data):
    """`{sink: [series name]}`: each sink's exported series whose `label` names no instance, so
    no instance's V holds it."""
    label = data.overlap.get("label")
    found = {}
    for sink, series in data.vm_series.items():
        names = [vm.series_name(one) for one in series
                 if (one.get("metric") or {}).get(label) not in data.instances]
        if names:
            found[sink] = names
    return found


def _life_cumulative(tel, selections):
    """A function of time: the running total of the `sum` points `selections`, `[(name,
    attrs)]`, select, from 0 at the first life's start, held flat from a life's last drain to
    the next life's start, and linear between drains, so an instance that is down reads
    flat."""
    by_ts = {}
    for name, attrs in selections:
        for ts, value, _ in tel.counter_points(name, **attrs):
            by_ts[ts] = by_ts.get(ts, 0) + value
    marks = sorted([(start, 0) for start in tel.life_starts]
                   + [(ts, 1) for ts in set(tel.drains) | set(by_ts)])
    anchors, total = [], 0.0
    for t, is_drain in marks:
        if is_drain:
            total += by_ts.get(t, 0)
        anchors.append((t, total))
    stamps = [t for t, _ in anchors]

    def at(t):
        if not anchors or t <= anchors[0][0]:
            return 0.0
        index = bisect.bisect_left(stamps, t)
        if index >= len(anchors):
            return anchors[-1][1]
        (t1, c1), (t2, c2) = anchors[index - 1], anchors[index]
        return c1 if t2 == t1 else c1 + (c2 - c1) * (t - t1) / (t2 - t1)
    return at


def _overlap_ingest(leds, source):
    """`(drains, counts, counted, start)` for `_wire_buckets` over every instance at once: each
    instance's W + K + F for the source's listener, and F alone, as running totals
    (`_life_cumulative`) summed and differenced at every instance's drains, since the
    instances drain out of phase. `start` is the earliest instance's first life start."""
    listener = source.get("sut_listener")
    wire = WIRES[source_wire(source)]
    lost = [("logit.input.frames.dropped", {"component": listener, "reason": reason})
            for reason in wire.counted]
    read = ([(wire.w, {"component": listener})]
            + ([(wire.k, {"component": listener})] if wire.k else []) + lost)
    totals = [_life_cumulative(led.ingress, read) for led in leds]
    framed = [_life_cumulative(led.ingress, lost) for led in leds]
    starts = [led.inn.life_starts[0] for led in leds if led.inn.life_starts]
    start = min(starts) if starts else 0.0
    drains = sorted({d for led in leds for d in led.ingress.drains if d > start})
    counts, counted = {}, {}
    previous = start
    for drain in drains:
        counts[drain] = sum(f(drain) for f in totals) - sum(f(previous) for f in totals)
        counted[drain] = sum(f(drain) for f in framed) - sum(f(previous) for f in framed)
        previous = drain
    return drains, counts, counted, start


def check_ledger_overlap(data):
    """Under `[overlap]`, else no row: on run totals, the generators' G less every instance's
    W + K + F, what no instance read or counted dropped, is at most what the instances' socket
    closes could discard, each stop's or restart's `ceil(used / TRUESIZE_FLOOR_B) + 1` datagrams,
    `used` being the closing life's last `receive_buffer.used.bytes`, sampled after its read loop
    stopped; and no more than the wire's end allowance below 0. The bound is loose by up to about
    a quarter of `SO_RCVBUF` (`udp_rmem_release` frees a socket's charge in batches), so the
    kernel can still charge datagrams it has handed over. A run with a kill or an `exit` of an
    instance, which closes with no final sample, or with another fault that can lose UDP on its
    way in, is reported and not judged. Each stop's own gap, read from the drain intervals that
    span it, is reported, never judged: interpolating the instances' out-of-phase drains errs by
    about a generator batch at each edge."""
    if not data.overlap:
        return []
    leds = _instance_ledgers(data)
    problems = [problem for led in leds.values() for problem in led.problems]
    if problems:
        return Result("ledger.overlap", SKIP, "; ".join(problems))
    first = leds[data.instances[0]]
    if not first.gens or any(source.get("service") not in first.gens
                             for source in first.sources):
        return Result("ledger.overlap", SKIP, "no generator telemetry")
    G = first.G
    read = {instance: sum(sum(led.per[listener][symbol].values()) for listener in led.listeners
                          for symbol in ("W", "K", "F"))
            for instance, led in leds.items()}
    gap = G - sum(read.values())
    buckets = {source.get("sut_listener"): _wire_buckets(
        data, first, source, combined=_overlap_ingest(list(leds.values()), source))
        for source in first.sources}
    instances = set(data.instances)
    lines, unjudged = [], []
    allowance = 0
    for fault in sorted(data.faults, key=lambda f: f.start):
        if fault.on in instances and fault.action in OVERLAP_CLOSES + OVERLAP_UNJUDGED:
            led = leds[fault.on]
            life = vm.life_at(led.inn.life_starts, fault.start)
            used, rcvbuf = [], 0
            for listener in led.listeners:
                series = [v for _, v, point_life in led.ingress.gauge_series(
                    USED_BYTES, component=listener) if point_life == life]
                buffer = [v for _, v, point_life in led.ingress.gauge_series(
                    "logit.input.receive_buffer.bytes", component=listener)
                          if point_life == life]
                used.append(series[-1] if series else None)
                rcvbuf = max(rcvbuf, buffer[-1] if buffer else 0)
            own = sum(g - wk for one in buckets.values() for _, _, _, label, g, wk, _ in one
                      if any(part.startswith(f"{fault.step} ") for part in label.split(", ")))
            head = (f"{fault.step} {fault.action} on {fault.on} at {data.offset(fault.start)}, "
                    f"life {life}: gap {_n(round(own))} in the drain intervals across it "
                    "(reported)")
            if fault.action in OVERLAP_UNJUDGED:
                unjudged.append(f"{fault.step} {fault.action} on {fault.on}")
                lines.append(f"{head}; a {fault.action} ends the process with no final sample, "
                             "so the close can't be bounded")
                continue
            if None in used:
                unjudged.append(f"{fault.step} {fault.action} on {fault.on} with no "
                                f"{USED_BYTES} sample in life {life}")
                lines.append(f"{head}; no {USED_BYTES} sample in the closing life")
                continue
            bound = sum(math.ceil(one / TRUESIZE_FLOOR_B) + 1 for one in used)
            allowance += bound
            loose = rcvbuf / 4 / TRUESIZE_FLOOR_B
            lines.append(f"{head}; last {USED_BYTES} {_n(sum(used))} -> at most {_n(bound)} "
                         f"datagram(s) discarded (loose by up to {_n(loose)} at a quarter of "
                         "its SO_RCVBUF)")
        elif any(fault.affects(source_wire(source)) for source in first.sources) and not (
                fault.on in instances and fault.action == "pause"):
            unjudged.append(f"{fault.step} {fault.action} on {fault.on}")
            lines.append(f"{fault.step} {fault.action} on {fault.on} at "
                         f"{data.offset(fault.start)}: can lose UDP on its way in, so the "
                         "run's gap isn't the closes' alone")
    below = sum(_end_allowance(first, source) for source in first.sources)
    split = ", ".join(f"{instance} {_n(value)}" for instance, value in read.items())
    totals = f"G {_n(G)} − Σ(W + K + F) {_n(sum(read.values()))} ({split}) = {_n(gap)}"
    if unjudged:
        return Result("ledger.overlap", SKIP,
                      f"{totals}, reported, not judged: {'; '.join(unjudged)}", lines)
    if gap > allowance:
        status, verdict = FAIL, f"uncounted, over the closes' {_n(allowance)}"
    elif gap < -below:
        status, verdict = FAIL, f"a surplus, past the end allowance {_n(below)}"
    else:
        status, verdict = PASS, f"within [-{_n(below)}, {_n(allowance)}]"
    closes = sum(1 for f in data.faults if f.on in instances and f.action in OVERLAP_CLOSES)
    return Result("ledger.overlap", status,
                  f"{totals}, {verdict}: {closes} close(s) of an instance's socket, each at most "
                  f"ceil(used / {TRUESIZE_FLOOR_B}) + 1 datagrams, loose by up to a quarter of "
                  "its SO_RCVBUF; below, the generators' end allowance", lines)




def affects_source(data, fault, service, wire=DEFAULT_WIRE):
    """Whether `fault` can lose lines on the `wire` of the source whose generator is `service`:
    a fault affecting that wire on the SUT or anywhere off the generators affects every source,
    and one on a generator affects that generator's source alone."""
    if not fault.affects(wire):
        return False
    return data.services.get(fault.on) != GENERATOR_ROLE or fault.on == service


def wire_judgment(fault, wire):
    """How `ledger.wire` takes the window of a `fault` that affects a `wire` source: `fault`,
    reported and never judged, or `judged`, judged as steady state is with a surplus allowed
    (`TCP_REPORTED_ACTIONS`, `HTTP_REPORTED_ACTIONS`). Every UDP and file window is reported.
    On TCP a partition is reported unless its record shows the container kept its address; on
    HTTP it is judged, since G counts only what the listener answered."""
    if wire == HTTP_WIRE:
        return "fault" if fault.action in HTTP_REPORTED_ACTIONS else "judged"
    if wire != "tcp" or fault.action in TCP_REPORTED_ACTIONS:
        return "fault"
    if fault.action == "partition" and (fault.ips is None or fault.ips != fault.ips_after):
        return "fault"
    return "judged"


def _wire_buckets(data, led, source, combined=None, anchors_out=None):
    """G − (W + K + F) for one source per SUT drain interval, classified `fault`, `judged`,
    `steady`, or `end`: its generator's G against its listener's W + K + F, with the fault
    windows that can affect its wire (`affects_source`), each reported or judged
    (`wire_judgment`).

    G is cumulative at each generator drain, anchored at 0 at the generator's start and held
    flat across a gap between its lives and across a drain that exports no G point, since a
    counter that didn't move in a drain has none, and is read at each SUT drain timestamp by linear
    interpolation, because the two processes' drains are out of phase. Within a run of steady
    intervals the interpolation telescopes; each edge of a run errs by at most one generator
    batch, which is the judgment's tolerance. Returns `[(start, end, kind, label, g, wkf, f)]`,
    `f` the part of `wkf` that is F. Under `[overlap]`, `combined` is every instance's W + K + F
    at once (`_overlap_ingest`). `anchors_out`, a list, receives G's `(timestamp, cumulative)`
    anchors."""
    service, listener = source.get("service"), source.get("sut_listener")
    wire_name = source_wire(source)
    wire = WIRES[wire_name]
    gen, sut = led.gens[service], led.ingress
    anchors = []
    cum = 0.0
    summed = {}
    for sink in source_ids(source, "generator_sink"):
        for ts, value, point_life in gen.counter_points(wire.g, component=sink):
            summed[(ts, point_life)] = summed.get((ts, point_life), 0) + value
    points = [(ts, value, point_life) for (ts, point_life), value in sorted(summed.items())]
    starts = list(gen.life_starts)
    for life, start in enumerate(starts):
        anchors.append((start, cum))
        # Every drain of the life is an anchor, a drain with no point at the running total:
        # without it a stalled writer's later G spreads back across the stall.
        moved = {ts: value for ts, value, point_life in points if point_life == life}
        for ts in sorted(set(gen.life_drains(life)) | set(moved)):
            cum += moved.get(ts, 0)
            anchors.append((ts, cum))
    anchors.sort(key=lambda item: item[0])
    anchor_ts = [t for t, _ in anchors]
    if anchors_out is not None:
        anchors_out.extend(anchors)

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

    if combined is not None:
        drains, counts, counted, previous = combined
    else:
        counts, counted = {}, {}
        for name in [wire.w] + ([wire.k] if wire.k else []):
            for ts, value, _ in sut.counter_points(name, component=listener):
                counts[ts] = counts.get(ts, 0) + value
        for reason in wire.counted:
            for ts, value, _ in sut.counter_points("logit.input.frames.dropped",
                                                   component=listener, reason=reason):
                counts[ts] = counts.get(ts, 0) + value
                counted[ts] = counted.get(ts, 0) + value
        if wire_name == HTTP_WIRE:
            # A batch the listener sent and answered `503` for, every consumer having closed,
            # is in W and its events in X, and the client still holds it, so G never counts it.
            for ts, value, _ in sut.counter_points("logit.component.events.dropped",
                                                   component=listener):
                counts[ts] = counts.get(ts, 0) - value
        drains = sorted(set(sut.drains) | set(counts))
        sut_starts = led.inn.life_starts
        previous = sut_starts[0] if sut_starts else (drains[0] if drains else 0)
    windows = []
    for fault in data.faults:
        if affects_source(data, fault, service, wire_name):
            ip = fault.ip_text()
            windows.append((fault.start, fault.end + data.recovery_bound,
                            f"{fault.step} {fault.action} on {fault.on}"
                            + (f" ({ip})" if ip else ""), wire_judgment(fault, wire_name)))
    buckets = []
    for drain in drains:
        if drain <= previous:
            continue
        hits = [(label, how) for start, end, label, how in windows
                if start < drain and end > previous]
        if hits:
            kind = "fault" if any(how == "fault" for _, how in hits) else "judged"
            label = ", ".join(label for label, _ in hits)
        elif drain > data.end:
            kind, label = "end", "end sequence"
        else:
            kind, label = "steady", f"no {wire.label}-affecting fault"
        buckets.append((previous, drain, kind, label, g_at(drain) - g_at(previous),
                        counts.get(drain, 0), counted.get(drain, 0)))
        previous = drain
    return buckets


def check_ledger_wire(data):
    """G − (W + K + F) per source, reported per run of drain intervals and judged outside the
    reported windows of faults that can affect that source's wire and at the end. On UDP that
    loss is by design and every affecting window is reported; on TCP F is the listener's counted
    framer drops, only a process end or a partition that moved an address is reported, and
    every other affecting window is judged with a surplus allowed for its ambiguous drops
    (`wire_judgment`); on HTTP G counts acknowledged points against W − X, only a kill or an
    exit is reported, and every other affecting window is judged with a surplus allowed for its
    transport-error attempts (`HTTP_REPORTED_ACTIONS`). See `_wire_buckets` for the tolerance.
    One detail per source; the worst status wins."""
    instances = None
    if _overlapped(data):
        leds = _instance_ledgers(data)
        problems = [problem for one in leds.values() for problem in one.problems]
        if problems:
            return Result("ledger.wire", SKIP, "; ".join(problems))
        instances = list(leds.values())
        led = instances[0]
    else:
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
        status, detail, source_lines = _wire_one(data, led, source, instances)
        statuses.append(status)
        label = f"{service} -> {source.get('sut_listener')}: " if len(led.sources) > 1 else ""
        details.append(label + detail)
        lines += [label + line for line in source_lines]
    return Result("ledger.wire", worst(statuses), " | ".join(details), lines)


def _boundary_nets(runs, anchors):
    """`(nets, netted)`: each run's gap, G − (W + K + F), after netting across every boundary
    that touches a judged run, judged or steady on its other side, where the two gaps have
    opposite signs, and `{index: [(moved, at, straddled)]}` for each run a transfer touched.

    G is interpolated between generator drains, so at a SUT drain inside a generator interval
    its error is up to the G that moved in that interval: one batch while acknowledgments arrive
    steadily, but many when they bunch, as under netem with one request in flight or when a
    restart flushes a backlog, and a few tenths of a second's shift in where a drain lands
    against that burst moves thousands of events across the boundary. The error moves G from one
    side of the boundary to the other, an opposite-signed pair in the two runs that touch, and
    nothing in the counts tells it from a resend beside a loss. So the pair nets in full before
    `_wire_one` applies the surplus run's resend allowance to what's left. A transfer is capped
    at the G that moved over the generator anchor pair straddling the boundary (`straddled`),
    the most interpolation can move, and at either gap, so neither run changes sign. That cap is
    what keeps a surplus from hiding a loss next door: a resend's surplus or a steady run's,
    which otherwise counts at 0, nets a loss only as far as interpolation could explain it.
    Runs that don't touch, and a reported or end run, never net."""
    nets = [g - wk for _, _, _, _, g, wk, _ in runs]
    netted = {}
    stamps = [t for t, _ in anchors]
    for index in range(len(runs) - 1):
        kinds = {runs[index][2], runs[index + 1][2]}
        if ("judged" not in kinds or not kinds <= {"judged", "steady"}
                or nets[index] * nets[index + 1] >= 0):
            continue
        at = runs[index][1]
        upper = bisect.bisect_left(stamps, at)
        if upper == 0 or upper >= len(anchors):
            continue
        straddled = anchors[upper][1] - anchors[upper - 1][1]
        loss, surplus = (index, index + 1) if nets[index] > 0 else (index + 1, index)
        moved = min(-nets[surplus], nets[loss], straddled)
        # An interpolated G leaves fractions; less than one event isn't worth a transfer.
        if moved < 1:
            continue
        sign = 1 if nets[index] > 0 else -1
        nets[index] -= sign * moved
        nets[index + 1] += sign * moved
        netted.setdefault(index, []).append((moved, at, straddled))
        netted.setdefault(index + 1, []).append((moved, at, straddled))
    return nets, netted


def _ambiguous_drops(led, source):
    """`[(timestamp, count)]` of what can leave one generator batch in W that G doesn't count,
    so `ledger.wire` and `_end_to_end` allow that much surplus per count: on a TCP wire the
    generator sink's `ambiguous_at_most_once` batch drops, each a write that failed partway, and
    on an HTTP wire its attempts that ended in an ambiguous transport error
    (`HTTP_AMBIGUOUS_CLASS`), each a request the listener may have taken before the answer was
    lost; a connect failure (`HTTP_CONNECT_CLASS`) sent nothing and isn't one. Empty on any
    other wire."""
    wire = source_wire(source)
    if wire == "tcp":
        name, attrs = "logit.component.batches.dropped", {"reason": AMBIGUOUS_DROP}
    elif wire == HTTP_WIRE:
        name, attrs = "logit.output.requests", {"class": HTTP_AMBIGUOUS_CLASS}
    else:
        return []
    return sorted((ts, v) for sink in source_ids(source, "generator_sink")
                  for ts, v, _ in led.gens[source.get("service")].counter_points(
                      name, component=sink, **attrs))


def _wire_one(data, led, source, instances=None):
    """`(status, detail, lines)` of `ledger.wire` for one source. Under `[overlap]`,
    `instances` holds each instance's ledger, and W, K, and F are theirs summed."""
    service, listener = source.get("service"), source.get("sut_listener")
    wire_name = source_wire(source)
    wire = WIRES[wire_name]
    udp = wire_name == DEFAULT_WIRE
    if wire_name == FILE_WIRE:
        return _file_wire_one(data, led, source)
    stats = led.gen_stats[service]
    status = PASS
    lines = []
    readers = instances or [led]
    W = sum(sum(one.per[listener]["W"].values()) for one in readers)
    K = sum(sum(one.per[listener]["K"].values()) for one in readers)
    F = sum(sum(one.per[listener]["F"].values()) for one in readers)
    G = stats["G"]
    http = wire_name == HTTP_WIRE
    # On HTTP, the events the listener refused after W counted them (`_wire_buckets`).
    X = sum(sum(one.per[listener]["X"].values()) for one in readers) if http else 0
    gap = G - W - K - F + X
    if udp and G != stats["G_datagrams"]:
        status = FAIL
        lines.append(f"precondition: generator output.messages {_n(G)} != output.datagrams "
                     f"{_n(stats['G_datagrams'])}, so a datagram isn't one line")
    elif wire_name == "tcp" and stats["G_datagrams"]:
        status = FAIL
        lines.append(f"precondition: generator output.datagrams {_n(stats['G_datagrams'])} "
                     "isn't 0, so its sink isn't sending over TCP")

    anchors = []
    buckets = _wire_buckets(data, led, source, combined=(
        None if instances is None else _overlap_ingest(instances, source)),
        anchors_out=anchors)
    runs = []
    for start, end, kind, label, g, wk, f in buckets:
        if runs and runs[-1][2] == kind and runs[-1][3] == label:
            runs[-1][1] = end
            runs[-1][4] += g
            runs[-1][5] += wk
            runs[-1][6] += f
        else:
            runs.append([start, end, kind, label, g, wk, f])
    sums = {"fault": 0.0, "judged": 0.0, "steady": 0.0, "end": 0.0}
    batch = stats["batch"]
    ambiguous = _ambiguous_drops(led, source)
    allowances = {}
    for index, (start, end, kind, _, _, _, _) in enumerate(runs):
        if kind == "judged":
            # A judged run's surplus has no pool to fall into, so every edge it has, a boundary
            # with another judged run included, allows its one batch of interpolation error.
            sides = sum(1 for other in (index - 1, index + 1) if 0 <= other < len(runs))
            drops = sum(v for ts, v in ambiguous if start < ts <= end)
            allowances[index] = (drops + sides) * batch
    connects = (sorted((ts, v) for sink in source_ids(source, "generator_sink")
                       for ts, v, _ in led.gens[service].counter_points(
                           "logit.output.requests", component=sink,
                           **{"class": HTTP_CONNECT_CLASS}))
                if http else [])
    # Each steady or judged run counts at 0 when W + K + F exceeds G in it, so a surplus in one
    # run can't net out loss in another. The one exception is a boundary that touches a judged
    # run, where an opposite-signed pair nets in full, up to the G that moved over the generator
    # interval straddling it, before the surplus run's allowance applies to what's left
    # (`_boundary_nets` says why).
    nets, netted = _boundary_nets(runs, anchors)
    steady_lost = 0.0
    g_steady = 0.0
    edges = 0
    surplus = []
    for index, (start, end, kind, label, g, wk, f) in enumerate(runs):
        sums[kind] += g - wk
        span = f"[{data.offset(start)}, {data.offset(end)}] {kind} ({label})"
        if udp:
            lines.append(f"{span}: G {_n(round(g))}, W+K {_n(wk)}, gap {_n(round(g - wk))}")
        elif http:
            lines.append(f"{span}: G {_n(round(g))}, W − X {_n(wk)}, gap {_n(round(g - wk))}")
        else:
            lines.append(f"{span}: G {_n(round(g))}, W {_n(wk - f)}, counted {_n(f)}, gap "
                         f"{_n(round(g - wk))}")
        for moved, at, straddled in netted.get(index, []):
            lines[-1] += (f", {_n(round(abs(moved)))} netted across the boundary at "
                          f"{data.offset(at)} (G moved {_n(round(straddled))} over the "
                          "generator interval straddling it)")
        net = nets[index]
        if index in netted:
            lines[-1] += f", net gap {_n(round(net))}"
        if kind not in ("steady", "judged"):
            continue
        steady_lost += max(0.0, net)
        g_steady += g
        run_edges = sum(1 for other in (index - 1, index + 1)
                        if 0 <= other < len(runs) and runs[other][2] != kind)
        edges += run_edges
        if kind == "judged":
            drops = sum(v for ts, v in ambiguous if start < ts <= end)
            sides = sum(1 for other in (index - 1, index + 1) if 0 <= other < len(runs))
            allowed = allowances[index]
            what = "transport-error attempt(s)" if http else "ambiguous drop(s)"
            dials = sum(v for ts, v in connects if start < ts <= end)
            lines[-1] += (f", surplus allowed {_n(allowed)} ({_n(drops)} {what} + "
                          f"{sides} edge(s), x one {_n(batch)}-line batch"
                          + (f"; {_n(dials)} connect failure(s) allow none" if dials else "")
                          + ")")
            if -net > allowed:
                took = "W − X" if http else "W + counted"
                surplus.append(f"{span}: {took} exceeds G by {_n(round(-net))}, over "
                               f"the {_n(allowed)} its {_n(drops)} {what} and "
                               f"{sides} edge(s) allow")
    if surplus:
        status = FAIL
        lines[:0] = surplus

    limit = float(data.ledger.get("wire_loss_outside_faults", 0.0))
    tolerance = batch * edges
    judged = steady_lost + max(0.0, sums["end"])
    allowed = limit * g_steady + tolerance
    if judged > allowed:
        status = FAIL
        outside = "UDP-affecting windows" if udp else f"reported {wire.label} windows"
        lines.insert(0, f"{_n(round(judged))} lost outside {outside}, over "
                        f"{limit:g} x {_n(round(g_steady))} + {edges} edge(s) x one "
                        f"{_n(batch)}-line batch")
    end_allow = _end_allowance(led, source)
    if sums["end"] < -end_allow:
        status = FAIL
        lines.insert(0, f"G falls {_n(round(-sums['end']))} short of W + K at the end, over "
                        f"the end allowance "
                        f"{_n(end_allow)} (the generator sink's last buffer.batches + 1 batch)")
    if udp:
        detail = (f"G {_n(G)} = W {_n(W)} + K {_n(K)} + wire {_n(gap)} (by design: "
                  f"{_n(round(sums['fault']))} in UDP-affecting windows, "
                  f"{_n(round(sums['steady']))} steady, {_n(round(sums['end']))} at the end; "
                  f"steady loss judged {_n(round(steady_lost))}, each run at 0 or more, against "
                  f"limit {_n(round(allowed))}); the generator's counted drop_newest loss, "
                  f"not in G: {_n(stats['dropped_events'])} event(s) in "
                  f"{_n(stats['dropped_batches'])} batch(es)")
        if instances is not None:
            detail += "; W by instance: " + ", ".join(
                f"{one.service} {_n(sum(one.per[listener]['W'].values()))}"
                for one in instances)
    elif http:
        one = led.per[listener]
        deferred = sum(one.get("deferred", {}).values())
        busy = sum(one.get("requests_busy", {}).values())
        retries = (f"{_n(stats['answered_5xx'])} answered 5xx and retried; the listener's busy "
                   f"503s {_n(busy)} and batches.deferred {_n(deferred)}, a retry, not loss"
                   if listener in led.http_kinds
                   and HTTP_LISTENERS[led.http_kinds[listener]].deferred
                   else f"{_n(stats['answered_5xx'])} answered 5xx and retried")
        detail = (f"G {_n(G)} (acknowledged of {_n(stats['attempted'])} attempted) = W "
                  f"{_n(W)} − X {_n(X)} + gap {_n(gap)} ({_n(round(sums['fault']))} in reported "
                  f"windows, {_n(round(sums['judged']))} in judged {wire.label}-affecting "
                  f"windows, {_n(round(sums['steady']))} steady, {_n(round(sums['end']))} at "
                  f"the end; loss judged {_n(round(steady_lost))}, each run at 0 or more, "
                  f"against limit {_n(round(allowed))}); the generator's requests: "
                  f"{_n(sum(v for _, v in ambiguous))} transport-error attempt(s), "
                  f"{_n(sum(v for _, v in connects))} connect failure(s), {retries}; "
                  f"its counted loss, not in G: {_n(stats['dropped_events'])} event(s) in "
                  f"{_n(stats['dropped_batches'])} batch(es)"
                  + (f" ({_reasons(stats['dropped_by'])})" if stats["dropped_by"] else ""))
    else:
        framed = {reason: sum(led.ingress.counter_by_life(
            "logit.input.frames.dropped", component=listener, reason=reason).values())
            for reason in wire.counted}
        framed_text = ", ".join(f"{reason} {_n(n)}" for reason, n in framed.items() if n)
        detail = (f"G {_n(G)} = W {_n(W)} + K {_n(K)} + counted {_n(F)}"
                  + (f" ({framed_text})" if framed_text else "")
                  + f" + gap {_n(gap)} ({_n(round(sums['fault']))} in reported windows, "
                  f"{_n(round(sums['judged']))} in judged {wire.label}-affecting windows, "
                  f"{_n(round(sums['steady']))} steady, {_n(round(sums['end']))} at the end; "
                  f"loss judged {_n(round(steady_lost))}, each run at 0 or more, against limit "
                  f"{_n(round(allowed))}); the generator's counted loss, not in G: "
                  f"{_n(stats['dropped_events'])} event(s) in {_n(stats['dropped_batches'])} "
                  f"batch(es)" + (f" ({_reasons(stats['dropped_by'])})"
                                  if stats["dropped_by"] else ""))
    if status != PASS:
        detail = f"{lines[0]}; {detail}"
    return status, detail, lines


def _end_allowance(led, source):
    """How far a source's G may fall short of W + K at the end: a send each generator sink
    cancels at shutdown loses its counts, up to that sink's last `buffer.batches` plus one, each
    a batch of the input that feeds it."""
    gen = led.gens[source.get("service")]
    sizes = led.gen_stats[source.get("service")]["sink_batch"]
    allowed = 0.0
    for sink in source_ids(source, "generator_sink"):
        series = gen.gauge_series("logit.component.buffer.batches", component=sink)
        allowed += ((series[-1][1] if series else 0) + 1) * sizes.get(sink, 0.0)
    return allowed


def _uncommented_text(text):
    return "\n".join(re.sub(r"(^|\s)#.*$", "", line) for line in (text or "").splitlines())


def _component_size(text, component, key):
    """A byte-size field of one component in a YAML text, block or flow style (`4MiB`,
    `65536`), in bytes, or None."""
    for block in component_blocks(text, component):
        found = re.search(rf"\b{key}\s*:\s*[\"']?(\d+)\s*([KMG]i?B|B)?\b", block)
        if found:
            return int(found.group(1)) * _SIZE_UNITS[found.group(2) or "B"]
    return None


def _file_writers(data, led, source):
    """`[(sink, rotations, max_files, max_bytes)]`, one per `file_out` writer of a file source:
    its `logit.output.file.rotations` over the run, its `rotate.max_files`
    (`DEFAULT_FILE_OUT_MAX_FILES` when unset), and its `rotate.max_bytes` in bytes, or None."""
    service = source.get("service")
    gen = led.gens[service]
    text = _uncommented_text(_service_config_text(data, service))
    return [(sink,
             int(sum(gen.counter_by_life("logit.output.file.rotations", component=sink).values())),
             component_int(text, sink, "max_files") or DEFAULT_FILE_OUT_MAX_FILES,
             _component_size(text, sink, "max_bytes"))
            for sink in source_ids(source, "generator_sink")]


def _file_retention_problems(data, led, source):
    """One problem per window of the ingress SUT's `FILE_RETENTION_ACTIONS` in which a file
    source's writer rotated often enough to delete a file the tailer hadn't read. `file_out`
    deletes `.{max_files − 1}` at each rotation, and each limit trips one rotation before the
    first such deletion:

    - A stop, kill, restart, or `exit` closes every file. A file rotated to `.1` shortly before
      the window, which a tailer one file behind hasn't finished, is deleted at the window's
      `max_files − 1`th rotation, so the limit is `max_files − 2`, and at least 1.
    - A pause keeps the tailer's descriptors, so a file it had open stays readable after a
      deletion. Only a file the writer created after the pause began is lost: created at the
      pause's first rotation, it's deleted at its `max_files + 1`th, so the limit is
      `max_files`.

    A restarted tailer never finds a deleted file, and the loss is counted only as far as the
    last checkpoint knew (docs/known-gaps/tailing.md), so the ledger can't see all of it any
    other way. Each window is
    widened by one of the writer's drain intervals on each side, since a rotation is counted at
    the drain after it."""
    gen = led.gens[source.get("service")]
    gaps = [b - a for a, b in zip(gen.drains, gen.drains[1:])]
    pad = statistics.median(gaps) if gaps else 0.0
    service = led.inn.service
    problems = []
    for sink, _, max_files, _ in _file_writers(data, led, source):
        for fault in data.faults:
            if fault.on != service or fault.action not in FILE_RETENTION_ACTIONS:
                continue
            limit = max_files if fault.action == "pause" else max(1, max_files - 2)
            rotated = gen.counter_in("logit.output.file.rotations", fault.start - pad,
                                     fault.end + pad, component=sink)
            if rotated >= limit:
                problems.append(
                    f"the writer rotated {_n(rotated)} time(s) while the SUT was down "
                    f"({fault.action} [{data.offset(fault.start)}, {data.offset(fault.end)}]), "
                    f"enough to delete a file it hadn't read: {sink} keeps {max_files} files, "
                    f"so {limit} rotation(s) in a {fault.action} trip the guard")
    return problems


def _file_wire_one(data, led, source):
    """`(status, detail, lines)` of `ledger.wire` for a file source. A tailer reads behind its
    writer, so a drain interval's gap is lag, not loss: the runs are reported, and the row
    judges the run's W + F − G against `Ledger.replay_band`.

    The driver counts the lines left in the shared directory at the end (`shared_counted`).
    While the writer deleted no file, that count must equal G. Once `file_out` deleted one, at
    each rotation past its `max_files − 1`th, the deleted lines can't be counted, so G stands
    alone: the count must not exceed G, and the directory must hold `min(rotations + 1,
    max_files)` files per writer. The row also FAILs a SUT down long enough for the writer to
    delete a file it hadn't read (`_file_retention_problems`). The end sequence's wait for the
    tailers (`tail_caught_up`) is reported beside it."""
    service, listener = source.get("service"), source.get("sut_listener")
    stats = led.gen_stats[service]
    status = PASS
    lines = []
    W = sum(led.per[listener]["W"].values())
    F = sum(led.per[listener]["F"].values())
    G = stats["G"]
    writers = _file_writers(data, led, source)
    deleted = sum(max(0, rotations - (max_files - 1)) for _, rotations, max_files, _ in writers)
    kept = sum(min(rotations + 1, max_files) for _, rotations, max_files, _ in writers)
    counted = data.phases.get("shared_counted")
    problems = []
    if counted is None:
        problems.append("precondition: no shared_counted phase, so G has no count from the "
                        "shared directory to hold it to")
    else:
        got_lines, got_files = counted.get("lines", 0), counted.get("files", 0)
        if got_lines > G:
            problems.append(f"precondition: the shared directory holds {_n(got_lines)} "
                            f"line(s), more than the writer's events.delivered {_n(G)}")
        elif not deleted and got_lines != G:
            problems.append(f"precondition: the shared directory holds {_n(got_lines)} "
                            f"line(s) in {_n(got_files)} file(s), but the writer's "
                            f"events.delivered is {_n(G)}")
        if deleted and got_files != kept:
            problems.append(f"precondition: the writer's rotations leave {_n(kept)} file(s) "
                            f"in the shared directory, and it holds {_n(got_files)}")
        if deleted:
            note = (f"the writer deleted {_n(deleted)} file(s); G stands alone, "
                    f"{_n(G - got_lines)} line(s) above the shared directory's count")
            sizes = [max_bytes for _, _, _, max_bytes in writers]
            if got_lines and all(sizes):
                line_bytes = counted.get("bytes", 0) / got_lines
                deleted_bytes = sum(max_bytes * max(0, rotations - (max_files - 1))
                                    for _, rotations, max_files, max_bytes in writers)
                note += (f", against about {_n(round(deleted_bytes / line_bytes))} in "
                         f"{_n(deleted)} file(s) of max_bytes at {line_bytes:,.1f} bytes a "
                         "line (reported, not judged)")
            lines.append(note)
    problems += _file_retention_problems(data, led, source)
    if problems:
        status = FAIL
    caught = data.phases.get("tail_caught_up") or {}
    low, high, band_lines = led.replay_band([listener])
    runs = []
    for start, end, kind, label, g, wk, f in _wire_buckets(data, led, source):
        if runs and runs[-1][2] == kind and runs[-1][3] == label:
            runs[-1][1] = end
            runs[-1][4] += g
            runs[-1][5] += wk
            runs[-1][6] += f
        else:
            runs.append([start, end, kind, label, g, wk, f])
    for start, end, kind, label, g, wk, f in runs:
        lines.append(f"[{data.offset(start)}, {data.offset(end)}] {kind} ({label}): "
                     f"G {_n(round(g))}, W {_n(wk - f)}, counted {_n(f)}, gap "
                     f"{_n(round(g - wk))}")
    lines += band_lines
    over = W + F - G
    if not low <= over <= high:
        status = FAIL
        what = "uncounted loss" if over < low else "surplus"
        problems.insert(0, f"W + F − G {_n(over)} is {what}, outside the replay band "
                           f"[{_n(low)}, {_n(high)}]")
    lines = problems + lines
    wait = ("" if not caught else
            f"; the tailers {'reached G' if caught.get('reached') else 'stopped short'} at the "
            f"end with W {_n(caught.get('w') or 0)}")
    shared = ("uncounted" if counted is None else
              f"{_n(counted.get('lines', 0))} lines in {_n(counted.get('files', 0))} files"
              + (f", the writer having deleted {_n(deleted)}" if deleted else ""))
    detail = (f"G {_n(G)} (shared directory: {shared}) vs W {_n(W)} + counted {_n(F)}: "
              f"W + F − G {_n(over)} against the replay band [{_n(low)}, {_n(high)}]" + wait)
    if status != PASS:
        detail = f"{lines[0]}; {detail}"
    return status, detail, lines


def _per_life_lines(led):
    return [f"life {life}{' (final)' if life == led.final else ''}"
            f"{' (killed)' if life in led.killed else ''}: {led.symbols(life)}"
            for life in led.lives]


def _ingress_lines(led):
    """One line per life of the listeners' SUT service, with the listener symbols alone."""
    inn = led.inn
    return [f"{inn.service} life {life}{' (final)' if life == inn.final else ''}"
            f"{' (killed)' if life in inn.killed else ''}: {led.listener_symbols(life)}"
            for life in inn.lives]


def _http_intake(led, listener, life):
    """`[(problem, text)]`: an HTTP listener's own identities in `life` (`HTTP_LISTENERS`), each
    a problem when it doesn't hold. One request is one batch, so its `ok` and `closed_consumer`
    requests sum to its `batches.sent`, and a busy `503` sends nothing, so its `busy` requests
    equal its `batches.deferred`. A listener with no request counter has neither, and one whose
    kind the SUT config doesn't give is reported; either note's `problem` is None."""
    if listener not in led.http_kinds:
        return ([(None, "an http listener whose kind the SUT config doesn't give: no request "
                         "identities")]
                if any(source.get("sut_listener") == listener and source_wire(source) == HTTP_WIRE
                       for source in led.sources) else [])
    kind = led.http_kinds[listener]
    symbols = HTTP_LISTENERS[kind]
    if symbols.requests is None:
        return [(None, f"{kind} counts no requests: W − D == E + B alone")]
    out = []
    one = led.per[listener]
    ok = one["requests_ok"].get(life, 0)
    refused = one["requests_closed_consumer"].get(life, 0)
    batches = one["batches"].get(life, 0)
    out.append((ok + refused != batches,
                f"{_route_text(symbols)} ok {_n(ok)} + closed_consumer {_n(refused)} "
                f"{'==' if ok + refused == batches else '!='} batches.sent {_n(batches)}"
                + ("" if ok + refused == batches else
                   ", so a request isn't one batch")))
    if symbols.deferred:
        busy = one["requests_busy"].get(life, 0)
        deferred = one["deferred"].get(life, 0)
        out.append((busy != deferred,
                    f"busy {_n(busy)} {'==' if busy == deferred else '!='} batches.deferred "
                    f"{_n(deferred)}"
                    + ("" if busy == deferred else ", so a busy 503 isn't one deferred batch")))
    return out


def _route_text(symbols):
    """An `HttpListener`'s write-route requests, as text."""
    return f"{symbols.requests}{{route={symbols.route}}}"


def check_ledger_intake(data):
    """The final life's W − D == E + B per listener, and on an HTTP wire the listener's own
    identities (`_http_intake`) in every life a kill didn't end; each earlier life's residual (`Ledger.residual`), summed over
    the listeners, in [0, R], a sanity bound. `ledger.egress` ties that residual to V. A life
    whose stderr shutdown lines disagree with telemetry's shutdown drops WARNs. Under
    `split = "target"` the residual ends at the router (W − D − B − X − R_in), and a stopped
    earlier life's must be 0, since each sink's egress starts at its target."""
    if _overlapped(data):
        return _per_instance(data, check_ledger_intake)
    led = _ledger(data)
    skipped = _skip_without_sut("ledger.intake", led)
    if skipped:
        return skipped
    status = PASS
    lines = (_per_life_lines(led) if led.hop is None
             else _ingress_lines(led) + _per_life_lines(led))
    life = led.inn.final
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
        # An HTTP listener's identities hold in every life its telemetry ends cleanly, so each
        # is judged; a killed life loses its last interval's counts, so it's reported unjudged.
        for each in led.inn.lives:
            label = f"{'final ' if each == life else ''}life {each}{name}"
            found = _http_intake(led, listener, each)
            if each in led.inn.killed and each != life:
                if any(problem is not None for problem, _ in found):
                    parts.append(f"{label} (killed): request identities not judged")
                continue
            for problem, text in found:
                if problem:
                    status = FAIL
                    parts.insert(0, f"{label}: {text}")
                elif problem is not None or each == life:
                    parts.append(f"{label}: {text}")
    # Each earlier life's residual and its bound: with one SUT, `Ledger.residual` in [0, R];
    # across a hop, each side's own, the ingress SUT's up to its hop sink in [0, R] and the far
    # SUT's from `logit_in` on in [0, R_hop].
    if led.hop is None:
        judged = [("", led.lives[:-1], led.residual, led.residual_text, led.R, "R")]
    else:
        judged = [(f"{led.inn.service} ", led.inn.lives[:-1], led.ingress_residual,
                   "W − D − B − X − S_hop", led.R, "R"),
                  (f"{led.service} ", led.lives[:-1], led.residual, led.residual_text,
                   led.R_hop, "R_hop")]
    for label, earlier_lives, residual_of, text, bound, name in judged:
        for earlier in earlier_lives:
            residual = residual_of(earlier)
            if led.targeted and earlier not in led.inn.killed:
                # The router's side starts each sink's own egress, so nothing else judges what
                # a stopped life read short of the router. Its final drain runs after every
                # data node has exited, so it counts the whole life: the residual is 0.
                ok = residual == 0
                parts.append(f"life {earlier} (stopped): {text} {_n(residual)}"
                             + ("" if ok else ", uncounted, not 0"))
                if not ok:
                    status = FAIL
                continue
            ok = 0 <= residual <= bound
            parts.append(f"{label}life {earlier}: {text} {_n(residual)} "
                         f"{'within' if ok else 'outside'} [0, {name}]")
            if not ok:
                status = FAIL
    mismatches = [m for m in map(led.datagram_mismatch, led.inn.lives) if m]
    if mismatches:
        status = worst([status, WARN])
        parts += mismatches
    detail = "; ".join(parts) + f" ({led.R_text}"
    if led.hop is not None:
        detail += f"; {led.R_hop_text}"
    return Result("ledger.intake", status, detail + ")", lines)


def _grouped(term):
    """`term` in parentheses when it is a sum, so a difference subtracts all of it."""
    return f"({term})" if " + " in term else term


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
    them, every transform sending each event it received that it didn't discard
    (received − discarded == sent), or for a `fan_out` node of k its children against
    k × received less the lines it dropped (`_node_edge`), and the last transform's sent in place
    of that listener's E in the sum; and out to each sink, under `copy` `aggregate`'s
    events.sent == the sink's events.received (a fan-out counts one send per batch, however
    many consumers it reaches), and under `route` the sink's target's events.sent == the
    sink's events.received. Under `target`, the edges `_target_edges` lists: into the router,
    the router's balance R_in + R_copied − R_disc == R_fwd + ΣT_sent, and each sink's branch
    from its target to it. Earlier lives are reported, except that the router's balance and
    its bounds (`Ledger.router_bounds`: each T_sent ≤ R_in − R_disc, and R_fwd ≤ R_in) are
    judged in every life a kill didn't end."""
    if _overlapped(data):
        return _per_instance(data, check_ledger_edge)
    led = _ledger(data)
    skipped = _skip_without_sut("ledger.edge", led)
    if skipped:
        return skipped
    inn, out = led.inn, led.out
    if led.hop is not None:
        hop_sink, hop_in = led.hop.get("sink"), led.hop.get("listener")
        edges = [("ΣE" if len(led.listeners) > 1 else "E", f"S_hop[{hop_sink}]", led.E,
                  led.S_hop, f"listeners to {hop_sink}", inn),
                 (f"E_B[{hop_in}]", "A", led.E_B, led.A, f"{hop_in} to aggregate", out)]
    elif led.script:
        edges = [("L_sent", "A", led.L_sent, led.A, "script to aggregate", out)]
    elif led.targeted:
        edges = _target_edges(led, out)
    else:
        edges = _chain_edges(led, out)
        into = {life: sum(led.into_aggregate(life).values()) for life in led.lives}
        if len(led.listeners) == 1 and not any(led.transforms.values()):
            edges.append(("E", "A", led.E, led.A, "listener to aggregate", out))
        else:
            edges.append(("ΣE" if not any(led.transforms.values()) else "Σ(sent into aggregate)",
                          "A", into, led.A, "listeners to aggregate", out))
    for sink in led.sinks if not led.targeted else ():
        received = led.S_in[sink]
        if led.split == "route":
            target = led.targets.get(sink)
            edges.append((f"T_sent[{target}]", f"S_in[{sink}]", led.T_sent.get(sink, {}),
                          received, f"target {target} to {sink}", out))
        else:
            edges.append(("A_sent", f"S_in[{sink}]", led.A_sent, received,
                          f"aggregate to {sink}", out))
    lines = []
    parts = []
    off = []
    # Each edge is judged in the final life of the SUT it runs in, named only across a hop. An
    # edge with a seventh element, true, is judged in every life a kill didn't end as well.
    for edge in edges:
        left, right, lhs, rhs, what, view = edge[:6]
        every = len(edge) > 6 and edge[6]
        final = view.final
        where = f"{view.service} " if led.hop is not None else ""
        # A node's edge names its left side and writes it per life (`_node_edge`).
        name, shown = left if isinstance(left, tuple) else (left, None)

        def side(life, a, name=name, shown=shown):
            return shown(life) if shown else f"{name} {_n(a)}"

        for life in view.lives:
            a, b = lhs.get(life, 0), rhs.get(life, 0)
            judged = life == final or (every and life not in view.killed)
            lines.append(f"{what}, {where}life {life}{' (final)' if life == final else ''}: "
                         f"{side(life, a)}, {right} {_n(b)}, {name} − {_grouped(right)} {_n(a - b)}"
                         + ("" if judged else " (reported, not judged)"))
            if judged and life != final and a != b:
                off.append(f"life {life}: {side(life, a)} != {right} {_n(b)}, off by "
                           f"{_n(a - b)} ({what})")
        a, b = lhs.get(final, 0), rhs.get(final, 0)
        lead = f"{where}final life {final}: " if led.hop is not None else ""
        if a != b:
            off.append(f"{lead}{side(final, a)} != {right} {_n(b)}, off by {_n(a - b)} "
                       f"({what})")
        else:
            parts.append(f"{lead}{side(final, a)} == {right} {_n(b)} ({what})")
    if led.targeted:
        for life in [each for each in out.lives if each == out.final or each not in out.killed]:
            for text, ok in led.router_bounds(life):
                where = "" if life == out.final else f"life {life}: "
                if ok:
                    if life == out.final:
                        parts.append(text)
                else:
                    off.append(f"{where}{text}, over its bound")
    head = "" if led.hop is not None else f"final life {led.final}: "
    if off:
        return Result("ledger.edge", FAIL, head + "; ".join(off + parts), lines)
    return Result("ledger.edge", PASS, head + "; ".join(parts), lines)


def _chain_edges(led, view):
    """Each source's `sut_transforms` hops for `ledger.edge`: its listener's E into the first
    node, each node's own term (`_node_edge`), and each node's T_out into the next."""
    edges = []
    for listener in led.listeners:
        left, lhs = f"E[{listener}]", led.per[listener]["E"]
        for component in led.transforms.get(listener) or []:
            edges.append((left, f"T_in[{component}]", lhs, led.T_in[component],
                          f"into {component}", view))
            edges.append(_node_edge(led, component, view))
            left, lhs = f"T_out[{component}]", led.T_out[component]
    return edges


def _target_edges(led, view):
    """`ledger.edge`'s edges under `split = "target"`, in path order: each source's chain, the
    listeners' sends into the router (R_in), the router's balance, judged in every life a kill
    didn't end, and per sink its branch: its target's sends less a closed consumer's drops into
    its first transform or its `aggregate`, each branch transform's own term (`_node_edge`),
    the last into the `aggregate` (A), and the `aggregate`'s sends into the sink."""
    edges = _chain_edges(led, view)
    into = {life: sum(led.into_aggregate(life).values()) for life in view.lives}
    single = len(led.listeners) == 1 and not any(led.transforms.values())
    edges.append(("E" if single else "Σ(sent into router)", f"R_in[{led.router}]", into,
                  led.R_in, f"listeners to router {led.router}", view))
    lives = view.lives
    balance = {life: led.router_balance(life) for life in lives}

    def told(life):
        return (f"R_in {_n(led.get('R_in', life))} + R_copied {_n(led.get('R_cp', life))} − "
                f"R_disc {_n(led.get('R_disc', life))} = {_n(balance[life][0])}")

    edges.append((("R_in + R_copied − R_disc", told), "R_fwd + ΣT_sent",
                  {life: balance[life][0] for life in lives},
                  {life: balance[life][1] for life in lives},
                  f"router {led.router} sends each event to every destination it names", view,
                  True))
    for sink in led.sinks:
        target, aggregate = led.targets.get(sink), led.aggregates.get(sink)
        t_sent, t_x = led.T_sent.get(sink, {}), led.T_x[sink]
        left = f"T_sent[{target}] − T_x[{target}]"
        lhs = {life: t_sent.get(life, 0) - t_x.get(life, 0) for life in set(t_sent) | set(t_x)}
        for component in led.branches.get(sink) or []:
            edges.append((left, f"T_in[{component}]", lhs, led.T_in[component],
                          f"into {component}", view))
            edges.append(_node_edge(led, component, view))
            left, lhs = f"T_out[{component}]", led.T_out[component]
        edges.append((left, f"A[{aggregate}]", lhs, led.A_k[sink], f"into {aggregate}", view))
        edges.append((f"A_sent[{aggregate}]", f"S_in[{sink}]", led.A_sent_k[sink],
                      led.S_in[sink], f"{aggregate} to {sink}", view))
    return edges


def _node_edge(led, component, view):
    """A `sut_transforms` node's hop for `ledger.edge`: T_in − T_disc == T_out, every event it
    received sent on or counted discarded. A `fan_out` node of k instead has children
    T_out − (T_in − T_disc), the count ADR `transform-fan-out` gives, equal to
    k × T_in − T_lines, every parent's k points less the lines it dropped. The left side is a
    name and a per-life text beside its per-life values, so a further term joins all three
    here."""
    t_in, t_disc = led.T_in[component], led.T_disc[component]
    k = led.fan_out.get(component)
    if k:
        t_out, t_lines = led.T_out[component], led.T_lines[component]
        lives = set(t_in) | set(t_disc) | set(t_out) | set(t_lines)
        children = {life: t_out.get(life, 0) - t_in.get(life, 0) + t_disc.get(life, 0)
                    for life in lives}
        points = {life: k * t_in.get(life, 0) - t_lines.get(life, 0) for life in lives}

        def told(life):
            return (f"children[{component}] = T_out {_n(t_out.get(life, 0))} − (T_in "
                    f"{_n(t_in.get(life, 0))} − T_disc {_n(t_disc.get(life, 0))}) = "
                    f"{_n(children.get(life, 0))}")

        return ((f"children[{component}]", told), f"{k} x T_in − T_lines[{component}]",
                children, points, f"{component} emits {k} per event", view)
    lhs = {life: t_in.get(life, 0) - t_disc.get(life, 0) for life in set(t_in) | set(t_disc)}
    name = f"T_in[{component}] − T_disc[{component}]"

    def text(life):
        return (f"T_in[{component}] {_n(t_in.get(life, 0))} − T_disc[{component}] "
                f"{_n(t_disc.get(life, 0))} = {_n(lhs.get(life, 0))}")

    return ((name, text), f"T_out[{component}]", lhs, led.T_out[component],
            f"{component} sends what it doesn't discard", view)


def check_ledger_script(data):
    """For a `[ledger] sut_script`, else SKIP, the final life's script hop: E == L_in (the
    listener's edge), L_in == L_emit + L_drop + L_err (every event emitted, discarded, or lost
    to a script error), and L_sent == L_emit + L_flush (what it sent is what `process()` emitted plus what
    `flush()` returned). Earlier lives are reported."""
    if _overlapped(data):
        return _per_instance(data, check_ledger_script)
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
    """The final life's Ab == A; under `split = "target"` each sink's own `aggregate`'s, one
    line per sink."""
    if _overlapped(data):
        return _per_instance(data, check_ledger_aggregate)
    led = _ledger(data)
    if not led.targeted or _skip_without_sut("ledger.aggregate", led):
        return _final_equality(data, "ledger.aggregate", "Ab", "A", "every event absorbed")
    status, parts, lines = PASS, [], []
    life = led.final
    for sink in led.sinks:
        aggregate = led.aggregates.get(sink)
        ab, a = led.Ab_k[sink], led.A_k[sink]
        for each in led.lives:
            lines.append(f"{aggregate}, life {each}{' (final)' if each == life else ''}: Ab "
                         f"{_n(ab.get(each, 0))}, A {_n(a.get(each, 0))}"
                         + ("" if each == life else " (reported, not judged)"))
        part = f"{aggregate}: Ab {_n(ab.get(life, 0))}"
        if ab.get(life, 0) != a.get(life, 0):
            status = FAIL
            parts.insert(0, f"{part} != A {_n(a.get(life, 0))}, off by "
                            f"{_n(ab.get(life, 0) - a.get(life, 0))}")
        else:
            parts.append(f"{part} == A {_n(a.get(life, 0))}")
    return Result("ledger.aggregate", status, f"final life {life}: " + "; ".join(parts)
                  + " (every event absorbed)", lines)


def check_ledger_egress(data):
    """The egress gap per life and per egress group (`Ledger.egress_groups`: each sink under
    `copy` and `target`, the sinks' summed V under `route`), Ab − V for the final life and a
    killed one, and W − D − B − X − V for a stopped earlier one, or under `target` the sink's
    own Ab and, for a stopped earlier life, T_sent − T_x − V from its target
    (`Ledger.egress`): 0 passes; a positive gap with a batch that
    life's telemetry counts in `batches.dropped`, under any reason, at a node that can explain
    it (`Ledger.drop_credit`) is counted, never reconciled; a killed life's gap passes inside
    `Ledger.kill_band`; any other gap is uncounted loss, or a surplus below a killed life's
    band or below 0 on a Kafka target. A `FAULT_DROP_REASONS` drop in a life's credit FAILs it
    whatever its gap. A sink whose export has no series, or a Kafka topic with no records,
    can't pass."""
    if _overlapped(data):
        return _egress_overlap(data)
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
            ab_v = led.absorbed(life, sinks) - led.v(life, sinks)
            flush = f" (L_flush {_n(led.get('L_flush', life))})" if led.script else ""
            ab_text = (f"Ab[{led.aggregates.get(sinks[0])}]" if led.targeted else led.ab_text)
            absorbed = (f"{ab_text} {_n(led.absorbed(life, sinks))}{flush} − "
                        f"{led.v_text(life, sinks)}")
            residual_text = (f"{led.branch_text(sinks[0])} − Ab" if led.targeted
                             else led.residual_text)
            if life == led.final:
                part = f"life {life} (final): {absorbed} = {_n(gap)}, {verdict}"
            elif life in led.killed:
                _, _, band = led.kill_band(life, sinks)
                part = (f"life {life} (killed): {absorbed} = {_n(gap)}, {verdict} ({band}); "
                        f"residual {residual_text} {_n(led.residual(life, sinks))} at its "
                        "last drain, what the kill lost from the receive queue and inbox, by "
                        "design, not judged here; D has no shutdown part: a SIGKILL counts no "
                        "shutdown drops")
            else:
                whole = ("E_B − X_B − V" if led.hop is not None
                         else f"{led.branch_text(sinks[0])} − V" if led.targeted
                         else "W′ − V" if led.reshaped
                         else f"W − D − B − X − {led.discarded_text}V")
                part = (f"life {life}: {whole} {_n(gap)} = residual "
                        f"{_n(led.residual(life, sinks))} + {ab_text} − V {_n(ab_v)}, {verdict}")
            credit = led.drop_credit(life, sinks)
            if gap > 0 and not (life in led.killed and life != led.final):
                others = ""
                elsewhere = led.other_sinks_drops(life, sinks)
                if elsewhere:
                    others = f", not counting {_reasons(elsewhere)} at the other sinks"
                records = led.record_drops(life, sinks)
                if records:
                    others += f"; records dropped: {_reasons(records)}"
                part += f" (batches dropped: {_reasons(credit)}{others})"
            elif credit and life in led.killed and life != led.final:
                # A SIGKILL counts no shutdown drops, but the life's drains can carry others,
                # such as a queue's overflow during a sink outage before the kill.
                part += (f"; batches dropped: {_reasons(credit)}, reported, not judged: the "
                         "band judges a killed life's gap")
            cued = led.cued_text(life, sinks)
            if cued:
                part += f"; {cued}, counted"
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
                shape = ("OTLP metrics requests" if led.topic_format == topic.OTLP_PROTO
                         else "JSON objects")
                parts.insert(0, f"{_n(led.unparsed)} topic record(s) aren't {shape}")
        for sink in empty:
            status = FAIL
            if data.standin:
                parts.insert(0, f"the stand-in's record holds no stored series for {sink}")
                continue
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
                         f"{_n(led.V_total[sink])} (the sum of the distinct "
                         + ("data points' delta sums" if led.topic_format == topic.OTLP_PROTO
                            else "records' delta sums") + ")")
            continue
        if sink == led.counts_sink and data.standin_influxdb:
            counts = data.standin_counts
            lines.append(f"{sink}: {_n(len(counts.points))} stored point(s), "
                         f"{_n(len(counts.rewrites))} rewritten with the same value; V "
                         f"{_n(led.V_total[sink])} (the sum of the stored points' values, one "
                         "per series and timestamp)")
            continue
        if sink == led.counts_sink:
            counts = data.standin_counts
            lines.append(f"{sink}: {_n(len(counts.points) + len(counts.duplicates))} stored "
                         f"point(s), {_n(len(counts.points))} distinct, "
                         f"{_n(len(counts.duplicates))} duplicate copies; V "
                         f"{_n(led.V_total[sink])} (the sum of the distinct points' values)")
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
    errors are shown beside it, counted. With `sut_transforms`, E − A becomes E − ΣT_disc − A,
    and the transforms' discards are shown beside it, counted. A counted egress term is shown
    and not judged, but a `FAULT_DROP_REASONS` drop in its group's credit FAILs the row as it
    FAILs `ledger.egress`; on an external target the Ab − V term SKIPs, and the row judges the
    others. On a Kafka target V counts the topic's distinct records, so a duplicate never
    enters the sum. Across a hop, W − D − E − B and E − S_hop are the ingress SUT's final
    life's, E_B − A and the rest the far SUT's, and the row also FAILs any group whose V over
    the run exceeds G less every counted loss (`_end_to_end`), as it does with a file or an
    HTTP source."""
    if _overlapped(data):
        return _summary_overlap(data)
    led = _ledger(data)
    skipped = _skip_without_sut("ledger.summary", led)
    if skipped:
        return skipped
    if led.targeted:
        return _summary_targeted(data, led)
    life = led.final
    g = led.get
    first = led.inn.final
    terms = [("W − D − E − B", g("W", first) - led.D(first) - g("E", first) - g("B", first))]
    if led.hop is not None:
        terms += [("E − S_hop", g("E", first) - g("S_hop", first)),
                  ("E_B − A", g("E_B", life) - g("A", life))]
    elif led.script:
        terms += [
            ("E − L_in", g("E", life) - g("L_in", life)),
            ("L_in − L_emit − L_drop − L_err",
             g("L_in", life) - g("L_emit", life) - g("L_drop", life) - g("L_err", life)),
            ("L_emit + L_flush − L_sent", g("L_emit", life) + g("L_flush", life)
             - g("L_sent", life)),
            ("L_sent − A", g("L_sent", life) - g("A", life)),
        ]
    elif led.reshaped:
        # A fan-out doesn't telescope, so each of ledger.edge's chain terms is summed, then the
        # chains' ends into `aggregate`.
        chain = sum(lhs.get(life, 0) - rhs.get(life, 0)
                    for _, _, lhs, rhs, _, _ in _chain_edges(led, led.out))
        terms += [("ledger.edge's chain terms", chain),
                  ("Σ(sent into aggregate) − A",
                   sum(led.into_aggregate(life).values()) - g("A", life))]
    elif led.T_disc:
        terms.append(("E − ΣT_disc − A", g("E", life) - led.t_disc(life) - g("A", life)))
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
    elif led.T_disc and not led.reshaped:
        counted_script = f"; the transforms' ΣT_disc {_n(led.t_disc(life))}, counted"
    wire = ""
    # An overlap instance reads part of the generators' lines; `_summary_overlap` adds the wire.
    whole = data.overlap_instance is None and led.gens
    if whole and not led.streams:
        wire = (f"; wire G − (W + K) {_n(led.G - sum(led.W.values()) - sum(led.K.values()))} "
                "over the run, by design")
    elif whole:
        wire = (f"; wire G − (W + K + F) "
                f"{_n(led.G - sum(led.W.values()) - sum(led.K.values()) - sum(led.F.values()))}"
                f" over the run, F {_n(sum(led.F.values()))} counted; ledger.wire judges the "
                "rest")
    status = PASS if judged == 0 and not uncounted_egress and not faults else FAIL
    bound_text = ""
    lines = _per_life_lines(led)
    lead = f"final life {life}"
    if led.hop is not None:
        lead = (f"{led.inn.service} final life {first}, {led.service} final life {life}; the hop "
                "itself is ledger.hop's")
        lines = _ingress_lines(led) + lines
        bound_status, bound_text = _end_to_end(data, led)
        status = worst([status, bound_status])
        wire += f"; {bound_text}"
    if led.hop is None and any(source_wire(source) in (FILE_WIRE, HTTP_WIRE)
                               for source in led.sources):
        bound_status, bound_text = _end_to_end(data, led)
        status = worst([status, bound_status])
        wire += f"; {bound_text}"
    detail = f"{lead}: uncounted {_n(judged)} = " + " + ".join(shown) + counted_script + wire
    if (status == FAIL and (led.hop is not None or bound_text) and judged == 0
            and not uncounted_egress and not faults):
        detail = f"{bound_text}; {detail}"
    return Result("ledger.summary", status, detail, lines)


def _summary_targeted(data, led):
    """`ledger.summary` under `split = "target"`: the final life's W − D − E − B, then every
    `_target_edges` term (sent − received), the router's balance among them, then per sink
    A − Ab and Ab − V. Each term is judged on its own and must be 0, a counted Ab − V aside,
    so a surplus in one term or at one sink can't net out loss in another; uncounted is the
    sum of their sizes. The router's term pools its targets: events it sent to one target in
    place of another leave it 0 and every sink's terms balanced. `ledger.edge`'s
    `router_bounds` catch only a target receiving more than the router took in. A `FAULT_DROP_REASONS` drop in a sink's credit FAILs the row as it
    FAILs `ledger.egress`. From a file or HTTP wire, each sink's V over the run is also
    bounded by what the generators sent less every counted loss (`_end_to_end`): a target
    receives at most one part of each event."""
    life = led.final
    first = led.inn.final
    g = led.get
    terms = [("W − D − E − B", g("W", first) - led.D(first) - g("E", first) - g("B", first))]
    for left, right, lhs, rhs, _, view in (edge[:6] for edge in _target_edges(led, led.out)):
        name = left[0] if isinstance(left, tuple) else left
        terms.append((f"{name} − {_grouped(right)}", lhs.get(view.final, 0) - rhs.get(view.final, 0)))
    for sink in led.sinks:
        aggregate = led.aggregates.get(sink)
        terms.append((f"A − Ab[{aggregate}]", led.A_k[sink].get(life, 0)
                      - led.Ab_k[sink].get(life, 0)))
    off = [(name, value) for name, value in terms if value]
    shown = [f"{name} {_n(value)}" for name, value in off]
    uncounted = sum(abs(value) for _, value in off)
    faults = []
    for _, sinks in led.egress_groups():
        _, verdict, gap = led.egress(life, sinks)
        counted = verdict == "counted"
        if gap and not counted:
            uncounted += abs(gap)
        fault = led.fault_drops(life, sinks)
        if fault:
            faults.append(fault)
        shown.append(f"Ab[{led.aggregates.get(sinks[0])}] − V[{sinks[0]}] {_n(gap)}"
                     + (f" {verdict}" if counted else "")
                     + (f" (FAIL: {fault})" if fault else ""))
    status = PASS if uncounted == 0 and not faults else FAIL
    rest = len(terms) - len(off)
    detail = (f"final life {life}: uncounted {_n(uncounted)}, each term judged on its own: "
              + "; ".join(shown) + (f"; the other {rest} term(s) 0" if rest else ""))
    if led.T_disc and led.t_disc(life):
        detail += f"; the transforms' ΣT_disc {_n(led.t_disc(life))}, counted"
    if led.gens and not data.external:
        read = sum(led.W.values()) + sum(led.K.values()) + sum(led.F.values())
        detail += f"; wire G − (W + K + F) {_n(led.G - read)} over the run; ledger.wire's"
    if any(source_wire(source) in (FILE_WIRE, HTTP_WIRE) for source in led.sources):
        bound_status, bound_text = _end_to_end(data, led)
        status = worst([status, bound_status])
        detail += f"; {bound_text}"
    lines = _per_life_lines(led) + [f"{name}: {_n(value)}" for name, value in terms]
    return Result("ledger.summary", status, detail, lines)


def _egress_overlap(data):
    """`ledger.egress` per overlap instance, each against the series its label names; a series
    whose label names no instance FAILs the row, since no instance's V can hold it."""
    result = _per_instance(data, check_ledger_egress)
    if not data.has_vm:
        return result
    label = data.overlap.get("label")
    for sink, names in sorted(_unlabeled(data).items()):
        result.status = FAIL
        line = (f"{sink}: {_n(len(names))} series whose {label} names no instance, so no "
                f"instance's V holds them: {', '.join(names[:3])}")
        result.lines.insert(0, line)
        result.detail = f"{line} | {result.detail}"
    return result


def _summary_overlap(data):
    """`ledger.summary` under `[overlap]`: each instance's final-life terms, then the wire over
    the run and one bound end to end, which no instance's split can loosen: each egress group's
    V summed over the instances is at most what the generators sent less every instance's
    counted loss (K, D, B, X, F, and the path's counted discards)."""
    result = _per_instance(data, check_ledger_summary)
    leds = _instance_ledgers(data)
    if any(led.problems for led in leds.values()):
        return result
    first = leds[data.instances[0]]
    if not first.gens:
        return result
    read = sum(sum(getattr(led, symbol).values()) for led in leds.values()
               for symbol in ("W", "K", "F"))
    counted = sum(sum(getattr(led, symbol).values()) for led in leds.values()
                  for symbol in ("K", "F", "D_tel", "B", "X"))
    counted += sum(led.discarded(life) for led in leds.values() for life in led.lives)
    limit = first.G - counted
    status, parts = PASS, []
    if not data.external:
        for label, sinks in first.egress_groups():
            total = sum(led.V_total.get(sink, 0) for led in leds.values() for sink in sinks)
            if total > limit:
                status = FAIL
                parts.append(f"{label}: ΣV {_n(total)} > G − counted {_n(limit)}, a surplus of "
                             f"{_n(total - limit)}")
            else:
                parts.append(f"{label}: ΣV {_n(total)} <= G − counted {_n(limit)}")
    bound = (f"wire G − Σ(W + K + F) {_n(first.G - read)} over the run, ledger.overlap's; "
             f"end to end over the run, G {_n(first.G)} less the instances' counted loss "
             f"{_n(counted)}: " + ("; ".join(parts) or "no V on this target"))
    detail = f"{result.detail}; {bound}"
    if status == FAIL and result.status != FAIL:
        detail = f"{bound}; {result.detail}"
    return Result("ledger.summary", worst([result.status, status]), detail, result.lines)


def _end_to_end(data, led):
    """`(status, text)`: across a hop or from a file or HTTP wire, each egress group's V over
    the whole run against what the generators sent less every counted loss on the way (K, D, B,
    X, the hop sink's drops, X_B, and the path's counted discards; on a TCP or file wire F too;
    on an HTTP wire no X, which the client resends), plus one generator batch per ambiguous drop
    on a TCP wire and per transport-error attempt on an HTTP wire, as `ledger.wire` allows, and
    on a file wire the replay band's upper edge. With a `fan_out`, each source's share is
    carried through its chain instead (`Ledger.through`). A kill on either side shifts lives but
    never adds increments, so V above that is a surplus whatever was killed; V below it is the
    per-life rows' to judge."""
    counted = sum(sum(getattr(led, symbol).values())
                  for symbol in ("K", "F", "D_tel", "B", "X"))
    counted -= sum(sum(led.per[source.get("sut_listener")]["X"].values())
                   for source in led.sources if source_wire(source) == HTTP_WIRE)
    # A replayed line's discard counts here as its read counts in W, so the band still bounds
    # the surplus.
    counted += sum(led.discarded(life) for life in led.lives)
    if led.hop is not None:
        counted += sum(led.hop_dropped.values()) + sum(led.X_B.values())

    def allowance(wire):
        return sum(sum(v for _, v in _ambiguous_drops(led, source))
                   * led.gen_stats[source.get("service")]["batch"]
                   for source in led.sources
                   if source_wire(source) == wire and source.get("service") in led.gens)

    partial, resent = allowance("tcp"), allowance(HTTP_WIRE)
    https = any(source_wire(source) == HTTP_WIRE for source in led.sources)
    # A file wire's tailers replay lines after a kill and split one line per file at a stop,
    # so V may exceed G by the replay band's upper edge.
    files = [source.get("sut_listener") for source in led.sources
             if source_wire(source) == FILE_WIRE]
    replay = led.replay_band(files)[1] if files else 0
    limit = led.G - counted + partial + resent + replay
    bound = ("G − counted" + (" + partial" if partial else "") + (" + resent" if https else "")
             + (" + replay" if files else ""))
    carried = ""
    if led.reshaped:
        # Each source's own share of the limit, in its listener's events, carried through its
        # chain over the run: V counts what `aggregate` absorbed, k per event past a fan-out
        # and none for a discarded one.
        lives = sorted(set(led.lives) | set(led.inn.lives))
        limit = 0
        for source in led.sources:
            listener, service = source.get("sut_listener"), source.get("service")
            one = led.per[listener]
            share = led.gen_stats.get(service, {}).get("G", 0) - sum(
                sum(one[symbol].values()) for symbol in ("K", "F", "D_tel", "B")
                + (() if source_wire(source) == HTTP_WIRE else ("X",)))
            if service in led.gens and source_wire(source) in ("tcp", HTTP_WIRE):
                share += (sum(v for _, v in _ambiguous_drops(led, source))
                          * led.gen_stats[service]["batch"])
            if source_wire(source) == FILE_WIRE:
                share += led.replay_band([listener])[1]
            limit += led.through(listener, share, lives)
        bound = f"({bound}) carried through the chains"
        carried = f", carried through the chains' fan_out and discards to {_n(limit)}"
    status, parts = PASS, []
    for label, sinks in led.egress_groups():
        total = sum(led.V_total.get(sink, 0) for sink in sinks)
        if total > limit:
            status = FAIL
            parts.append(f"{label}: V {_n(total)} > {bound} {_n(limit)}, a surplus of "
                         f"{_n(total - limit)}")
        else:
            parts.append(f"{label}: V {_n(total)} <= {bound} {_n(limit)}")
    return status, ("end to end over the run, G " + _n(led.G) + " less counted loss "
                    + _n(counted) + (f" and a partial-frame allowance {_n(partial)}"
                                     if partial else "")
                    + (f" plus a resend allowance {_n(resent)}" if https else "")
                    + (f" plus the replay band's {_n(replay)}" if files else "")
                    + carried + ": " + "; ".join(parts))


def _last_interval(tel, name, component, life):
    """`(tail, rate)`: a counter's events in a life's last drain interval, scaled to its median
    drain interval, and the rate it ran at: what a kill after that drain can leave uncounted."""
    drains = tel.life_drains(life)
    gaps = [b - a for a, b in zip(drains, drains[1:])]
    if not gaps or gaps[-1] <= 0:
        return 0, 0.0
    last = sum(v for ts, v, point_life in tel.counter_points(name, component=component)
               if point_life == life and ts == drains[-1])
    rate = last / gaps[-1]
    return round(rate * statistics.median(gaps)), rate


# How many of a life's last drain intervals that moved a counter `_commit_rate` reads.
COMMIT_RATE_INTERVALS = 3


def _commit_rate(tel, name, component, life):
    """`(rate, last)`: the highest per-second rate a counter ran at over the last
    `COMMIT_RATE_INTERVALS` drain intervals of a life it moved in, and the timestamp of the
    last drain it moved in, or None. Delivery that stopped partway through the last interval
    averages low over it, so an earlier, whole interval sets the rate."""
    drains = tel.life_drains(life)
    by_ts = {ts: v for ts, v, point_life in tel.counter_points(name, component=component)
             if point_life == life}
    moved = [(a, b) for a, b in zip(drains, drains[1:]) if b > a and by_ts.get(b)]
    rate = max((by_ts[b] / (b - a) for a, b in moved[-COMMIT_RATE_INTERVALS:]), default=0.0)
    return rate, (moved[-1][1] if moved else None)


def _first_moved(tel, name, component, life):
    """The first drain of a life a counter moved in, or None."""
    return next((ts for ts, v, point_life in tel.counter_points(name, component=component)
                 if point_life == life and v), None)


def _cumulative(tel, name, component, minus=None):
    """A function of time: a counter's running total across every life, interpolated linearly
    between its drains and held flat between lives, less `minus`'s, if named."""
    by_ts = {}
    for ts, v, _ in tel.counter_points(name, component=component):
        by_ts[ts] = by_ts.get(ts, 0) + v
    if minus is not None:
        for ts, v, _ in tel.counter_points(minus, component=component):
            by_ts[ts] = by_ts.get(ts, 0) - v
    anchors, total = [], 0.0
    for ts in sorted(set(tel.drains) | set(by_ts)):
        total += by_ts.get(ts, 0)
        anchors.append((ts, total))
    stamps = [t for t, _ in anchors]

    def at(t):
        if not anchors or t <= anchors[0][0]:
            return 0.0
        index = bisect.bisect_left(stamps, t)
        if index >= len(anchors):
            return anchors[-1][1]
        (t1, c1), (t2, c2) = anchors[index - 1], anchors[index]
        return c1 if t2 == t1 else c1 + (c2 - c1) * (t - t1) / (t2 - t1)
    return at


def check_ledger_hop(data):
    """Across a `[[ledger.hop]]`, on run totals: the events the ingress SUT's hop sink
    received, less those it counted dropped, against the events the far SUT's `logit_in`
    forwarded, gap = ΣS_hop − Σhop dropped − ΣE_B. Lives can't be paired across the two
    processes, so the row judges the whole run, and 0 passes. A killed life on either side
    loses up to one drain interval of its counts (`_last_interval`). For the ingress SUT that
    lowers S_hop, so the gap may fall by that much; its hop sink counts a batch received before
    it spools it, and a spooled batch survives the kill, so the gap may rise by only the one
    batch in hand, one frame. For the far SUT it lowers E_B, so the gap may rise by that much.
    A killed ingress life's spool also replays the frames it committed after its last cursor
    persist, up to its commit rate (`_commit_rate`) × `checkpoint_interval`, which the far SUT
    forwards again only if it restarted between that life's last delivery and the replay's,
    forgetting their marks; then the gap may fall by that many frames too. A kill or a pause of
    the far SUT can make it forward a resend twice: a kill forgets its marks, and a pause
    leaves frames in an ended connection that a resend races (docs/known-gaps/native-hop.md),
    each up to one send window of frames below 0. A partition or packet loss can't: the race
    needs a forward parked on a full inbox. Any other
    gap FAILs, as a surplus below the allowance or uncounted loss above it. Resends, resumed
    frames, and reconnects are shown beside it. The lines report the running gap at each
    fault's start, the hop's backlog plus any surplus so far, from both sides' running totals,
    which drain out of phase, so each errs by up to a drain interval of each. A surplus can't be
    told from a backlog while one is in flight, so no fault's share of the gap is judged. A run
    with no hop has no row."""
    if not data.ledger.get("hop"):
        return []
    led = _ledger(data)
    skipped = _skip_without_sut("ledger.hop", led)
    if skipped:
        return skipped
    inn, out = led.inn, led.out
    sink, listener = led.hop.get("sink"), led.hop.get("listener")
    received = sum(led.S_hop.values())
    dropped = sum(led.hop_dropped.values())
    forwarded = sum(led.E_B.values())
    gap = received - dropped - forwarded
    race = round(led.window * led.per_frame)
    low = high = 0
    allowances = []
    frame = round(led.per_frame)
    for life in inn.killed:
        tail, rate = _last_interval(led.ingress, "logit.component.events.received", sink, life)
        low, high = low - tail, high + frame
        text = (f"{inn.service} life {life} killed: −{_n(tail)} ({rate:,.0f}/s over its last "
                f"drain interval), +{_n(frame)} (the batch in hand)")
        commit, last = _commit_rate(led.ingress, "logit.component.batches.delivered", sink,
                                    life)
        replayed = (_first_moved(led.ingress, "logit.component.batches.delivered", sink,
                                 life + 1) if life + 1 in inn.lives else None)
        restarted = [start for start in out.life_starts[1:]
                     if last is not None and replayed is not None and last < start <= replayed]
        if restarted:
            replay = (math.ceil(commit * led.hop_checkpoint) + 1) * frame
            low -= replay
            text += (f", −{_n(replay)} (its replay of frames committed after its last cursor "
                     f"persist, which {out.service}, restarted at "
                     f"{data.offset(restarted[0])}, forwards again)")
        allowances.append(text)
    for life in out.killed:
        tail, rate = _last_interval(led.sut, "logit.component.events.sent", listener, life)
        low, high = low - race, high + tail
        allowances.append(f"{out.service} life {life} killed: −{_n(race)} (its marks lost), "
                          f"+{_n(tail)} ({rate:,.0f}/s over its last drain interval)")
    for fault in data.faults:
        if fault.on == out.service and fault.action == "pause":
            low -= race
            allowances.append(f"{fault.step} pause on {out.service}: −{_n(race)} (a resend "
                              "racing an ended connection's frames)")
    resends, resumed = sum(led.resends.values()), sum(led.resumed.values())
    reconnects = sum(led.reconnects.values())
    beside = (f"resends {_n(resends)} at {out.service}, resumed {_n(resumed)} and reconnects "
              f"{_n(reconnects)} at {inn.service}; a send window is {led.window} frames of at "
              f"most {led.per_frame:,.0f} events ({_n(race)})")
    sent_by = _cumulative(led.ingress, "logit.component.events.received", sink,
                          minus="logit.component.events.dropped")
    taken_by = _cumulative(led.sut, "logit.component.events.sent", listener)
    lines = []
    for fault in sorted(data.faults, key=lambda f: f.start):
        held = sent_by(fault.start) - taken_by(fault.start)
        lines.append(f"at {fault.step} {fault.action} on {fault.on} "
                     f"({data.offset(fault.start)}): {_n(round(held))} not yet forwarded")
    if data.end:
        lines.append(f"at the end sequence ({data.offset(data.end)}): "
                     f"{_n(round(sent_by(data.end) - taken_by(data.end)))} not yet forwarded")
    totals = (f"ΣS_hop {_n(received)} − hop dropped {_n(dropped)} − ΣE_B {_n(forwarded)} = "
              f"{_n(gap)}")
    allowed = (f"allowance [{_n(low)}, {_n(high)}]: " + "; ".join(allowances)
               if allowances else "no kill or pause of either side, so 0")
    if gap == 0:
        status, verdict = PASS, "ok"
    elif low <= gap <= high:
        status, verdict = PASS, "within allowance"
    else:
        status, verdict = FAIL, "surplus" if gap < low else "uncounted"
    detail = f"{sink} to {listener}: {totals}, {verdict} ({allowed}); {beside}"
    # A counted drop leaves the gap, but one that says the hop failed FAILs it, as
    # `ledger.egress` FAILs one at a sink.
    drops = _by_attr(led.ingress, "logit.component.batches.dropped", "reason", component=sink)
    fault = ", ".join(f"{_n(drops[reason])} batch(es) {what}"
                      for reason, what in FAULT_DROP_REASONS.items() if drops.get(reason))
    if fault:
        status = FAIL
        detail = f"FAIL: {fault}; {detail}"
    return Result("ledger.hop", status, detail, lines)


def _torn_upto(sut, sink, life, at):
    """`batches.dropped{reason="disk_torn"}` for one sink and life up to `at`. A spool counts it
    at open for a batch an earlier life received and lost with a torn tail, so like
    `buffer.disk.replayed` it settles a batch this life never received."""
    return sum(v for ts, v, point_life in sut.counter_points(
        "logit.component.batches.dropped", component=sink, reason="disk_torn")
        if point_life == life and ts <= at)


def check_identity_sink(data):
    """For each sink, at the final life's last drain before its shutdown drain:
    batches.received + buffer.disk.replayed + batches.dropped{reason="disk_torn"} ==
    delivered + dropped (every reason) + buffer.batches, one batch in flight allowed."""
    if _overlapped(data):
        return _per_instance(data, check_identity_sink)
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
        torn = _torn_upto(sut, sink, life, at)
        delivered = upto("logit.component.batches.delivered")
        dropped = upto("logit.component.batches.dropped")
        queued = next((v for ts, v, point_life in reversed(sut.gauge_series(
            "logit.component.buffer.batches", component=sink))
            if point_life == life and ts <= at), 0)
        gap = received + replayed + torn - delivered - dropped - queued
        replay = (f" + replayed {_n(replayed)}" if replayed else "") + (
            f" + torn {_n(torn)}" if torn else "")
        if not 0 <= gap <= 1:
            status = FAIL
        lines.append(f"{sink}: received {_n(received)}{replay} vs delivered {_n(delivered)} + "
                     f"dropped {_n(dropped)} + buffer.batches {_n(queued)}, gap {_n(gap)}")
    detail = (f"final life {life} at {data.offset(at)}: " + "; ".join(lines)
              + " (one batch in flight allowed)")
    if led.hop is not None:
        # The hop sink, at its own SUT's final life's last quiet drain.
        tel, hop_life, sink = led.ingress, led.inn.final, led.hop.get("sink")
        hop_drains = tel.life_drains(hop_life)
        if len(hop_drains) < 2:
            return Result("identity.sink", SKIP, f"{led.inn.service} final life {hop_life} has "
                          "under two drains")
        hop_at = hop_drains[-2]

        def hop_upto(name):
            return sum(v for ts, v, point_life in tel.counter_points(name, component=sink)
                       if point_life == hop_life and ts <= hop_at)

        queued = next((v for ts, v, point_life in reversed(tel.gauge_series(
            "logit.component.buffer.batches", component=sink))
            if point_life == hop_life and ts <= hop_at), 0)
        replayed = hop_upto("logit.component.buffer.disk.replayed")
        torn = _torn_upto(tel, sink, hop_life, hop_at)
        delivered = hop_upto("logit.component.batches.delivered")
        dropped = hop_upto("logit.component.batches.dropped")
        received = hop_upto("logit.component.batches.received")
        gap = received + replayed + torn - delivered - dropped - queued
        if not 0 <= gap <= 1:
            status = FAIL
        line = (f"{led.inn.service} {sink}: final life {hop_life} at {data.offset(hop_at)}: "
                f"received {_n(received)} + replayed {_n(replayed)} + torn {_n(torn)} vs "
                f"delivered {_n(delivered)} + dropped {_n(dropped)} + buffer.batches "
                f"{_n(queued)}, gap {_n(gap)}")
        lines.append(line)
        detail = f"{led.service} {detail}; {line}"
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
    changes no total and only this row sees it. A stand-in target's V, rebuilt from its record
    in the export's shape, is judged the same way."""
    if _overlapped(data):
        return _per_instance(data, check_ledger_windows)
    if not (data.has_vm or data.standin):
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
    replay at open, so a short replay is lost windows `identity.sink` balances away.

    Across a hop, the ingress SUT's hop sink is judged for its own killed lives too. The
    listener feeds it directly, so its queue keeps growing between the last drain and the kill,
    and the far side may take some of it; its spool commits a frame once the far side
    acknowledges it and persists the cursor on a commit once `checkpoint_interval` has passed
    since the last persist, so up to one interval of commits before the last persist stays
    unpersisted, even while nothing is delivered, and replays again. With the rates over the
    life's last drain interval, t the time from its last drain to the `docker kill`'s return,
    and the highest delivered rate over its last few drain intervals that delivered anything
    (`_commit_rate`), replayed − queued is in [−(delivered rate × t) − 1, received rate × t +
    that commit rate × the checkpoint interval + 1]."""
    if _overlapped(data):
        return _per_instance(data, check_ledger_replay)
    led = _ledger(data)
    skipped = _skip_without_sut("ledger.replay", led)
    if skipped:
        return skipped
    judged = [(led.sut, sink, life, None) for sink in led.sinks for life in led.killed
              if life + 1 in led.lives]
    if led.hop is not None:
        judged += [(led.ingress, led.hop.get("sink"), life, led.inn.service)
                   for life in led.inn.killed if life + 1 in led.inn.lives]
    if not judged:
        return Result("ledger.replay", SKIP, "no killed life followed by another")
    status = PASS
    parts = []
    for sut, sink, life, service in judged:
        drains = sut.life_drains(life)
        nxt = sut.life_drains(life + 1)
        queued = next((v for ts, v, point_life in reversed(sut.gauge_series(
            "logit.component.buffer.batches", component=sink))
            if point_life == life and drains and ts <= drains[-1]), 0)
        replayed = sum(v for ts, v, point_life in sut.counter_points(
            "logit.component.buffer.disk.replayed", component=sink)
            if point_life == life + 1 and nxt and ts == nxt[0])
        low, high, band = -1, 1, ""
        if service is not None:
            _, sent = _last_interval(sut, "logit.component.batches.delivered", sink, life)
            _, took = _last_interval(sut, "logit.component.batches.received", sink, life)
            last, _ = _commit_rate(sut, "logit.component.batches.delivered", sink, life)
            done = led.inn.killed_done.get(life) or led.inn.killed_at[life]
            after = max(0.0, done - drains[-1]) if drains else 0.0
            low = -math.ceil(sent * after) - 1
            high = math.ceil(took * after + last * led.hop_checkpoint) + 1
            band = (f", band [{_n(low)}, {_n(high)}]: {took:,.1f} batches/s received and "
                    f"{sent:,.1f} delivered over its last drain interval, {after:.1f}s from "
                    f"its last drain to the kill, and a {led.hop_checkpoint:g}s checkpoint of "
                    f"its last delivering {last:,.1f}/s")
        ok = low <= replayed - queued <= high
        status = worst([status, PASS if ok else FAIL])
        where = f"{service} " if service is not None else ""
        parts.append(f"{where}{sink}: life {life + 1}'s first drain replayed {_n(replayed)} vs "
                     f"killed life {life}'s last-drain buffer.batches {_n(queued)}{band}"
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
    ledger goes on to the topic, as is a `datadog` stand-in's, whose ledger goes on to the
    stand-in's record and which excuses what a `respond` step cued (`_sent_one`). A local
    target and a `remote_write` or `influxdb` stand-in SKIP. `docs/plans/soak-harness.md`, "8. External
    targets (W5)", lists each FAIL and WARN."""
    if data.has_vm:
        return Result("ledger.sent", SKIP, "local target: ledger.egress judges delivery against "
                      "the backends")
    if data.standin and not data.standin_datadog:
        return Result("ledger.sent", SKIP, f"standin target, shape {data.standin_shape}: "
                      "ledger.egress and ledger.standin judge delivery against the stand-in's "
                      "record")
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
    drained = data.sut_phase("sink_drained", data.sut_out)
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
                "ledger.egress and ledger.standin read the stand-in's record" if data.standin
                else f"{NO_BACKEND}, so a 2xx is the end of the evidence")
    summary = "; ".join(summaries) + f" (one batch in flight allowed); {evidence}"
    if status == PASS:
        # `SENT` marks a ledger that ends at the sink; a Kafka or stand-in target's goes on to
        # what the destination holds.
        lead = "" if data.kafka or data.standin else "SENT "
        return Result("ledger.sent", PASS, f"{lead}to {data.target.get('name')}: every batch "
                      f"received ended in an accepted response; {summary}", lines)
    worst_first = sorted(problems, key=lambda item: -_ORDER[item[0]])
    return Result("ledger.sent", status, f"{worst_first[0][1]}; {summary}",
                  [text for _, text in worst_first] + lines)


def _sent_one(data, led, sink, flag, lines):
    """`ledger.sent`'s reading of one sink: flags each problem through `flag`, appends its
    lines, and returns its summary. A refusal line, a `rejected` batch drop, or a `rejected` or
    `oversize` record drop inside the window of a `respond` step that explains it
    (`Ledger.cued`, `cued_records`, and `cued_refusals`) is counted as cued and excused; only a
    stand-in target has such steps."""
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
        torn = _torn_upto(sut, sink, life, at)
        delivered = upto("logit.component.batches.delivered")
        dropped = upto("logit.component.batches.dropped")
        queued = next((v for ts, v, point_life in reversed(sut.gauge_series(
            "logit.component.buffer.batches", component=sink))
            if point_life == life and ts <= at), 0)
        gap = received + replayed + torn - delivered - dropped - queued
        replay = (f" + replayed {_n(replayed)}" if replayed else "") + (
            f" + torn {_n(torn)}" if torn else "")
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

    diagnostics = [entry for entry in data.stderr.get(led.service, [])
                   if entry.kind == "json" and entry.key in REJECTION_KEYS + REFUSAL_KEYS
                   and entry.component == sink]

    def quoted(*keys):
        """The first stderr line carrying one of `keys`, with the status it names."""
        first = next((entry for entry in diagnostics if entry.key in keys), None)
        if first is None:
            return f"no {' or '.join(keys)} line on stderr"
        return quote(first)

    def quote(first):
        """One stderr line, with the status it names."""
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

    cued = []
    starts = {fault.step: fault.start for fault in data.faults}

    def note_cued(what, excused):
        if excused:
            order = sorted(excused, key=lambda step: (starts.get(step, 0), step))
            cued.append(f"{what} " + ", ".join(f"{_n(excused[step])} by {step}"
                                               for step in order))

    for key in REFUSAL_KEYS:
        excused, outside = {}, []
        for entry in diagnostics:
            if entry.key != key:
                continue
            step = next((step for start, end, step in led.cued_refusals
                         if entry.ts is not None and start < entry.ts <= end), None)
            if step is None:
                outside.append(entry)
            else:
                excused[step] = excused.get(step, 0) + 1
        note_cued(f"{key} line(s)", excused)
        if outside:
            flag(FAIL, f"{sink}: the destination refused a request, so the sink held its "
                       f"queue: {len(outside)} {key} line(s)"
                       + (" outside every cued refused step" if excused else "")
                       + f" ({quote(outside[0])})")

    batch_drops = _by_attr(sut, "logit.component.batches.dropped", "reason", component=sink)
    for reason, value in sorted(batch_drops.items()):
        if reason in REJECTED_BATCH_REASONS:
            excused, _ = led.cued_split("logit.component.batches.dropped", [sink], reason,
                                        led.cued)
            note_cued(f"batch(es) dropped {reason}", excused)
            outside = value - sum(excused.values())
            if outside:
                flag(FAIL, f"{sink}: {_n(outside)} batch(es) dropped {reason}"
                           + (" outside every cued rejected step" if excused else "")
                           + f", the destination refused them ({quoted('request_rejected')})")
        elif data.external and reason in FAULT_DROP_REASONS:
            # On a Kafka target `ledger.egress` FAILs it instead.
            flag(FAIL, f"{sink}: {_n(value)} batch(es) {FAULT_DROP_REASONS[reason]}")
        else:
            flag(WARN, f"{sink}: {_n(value)} batch(es) dropped {reason}, counted")
    record_drops = _by_attr(sut, "logit.output.records.dropped", "reason", component=sink)
    for reason, value in sorted(record_drops.items()):
        if reason in REJECTED_RECORD_REASONS:
            excused, _ = led.cued_split("logit.output.records.dropped", [sink], reason,
                                        led.cued_records.get(reason, []))
            note_cued(f"record(s) dropped {reason}", excused)
            value -= sum(excused.values())
            if not value:
                continue
            # `rejected` counts both a rejected request's entries and the series a 2xx body
            # names, so it quotes whichever of the two the sink logged.
            groups = [("request_rejected", "oversize")]
            if reason == "rejected":
                groups.append(("series_rejected",))
            found = [quoted(*keys) for keys in groups
                     if any(entry.key in keys for entry in diagnostics)]
            evidence = "; ".join(found) or quoted(*groups[0])
            flag(FAIL, f"{sink}: {_n(value)} record(s) dropped {reason}"
                       + (" outside every cued step" if excused else "")
                       + f", refused by the destination or too large to send ({evidence})")
        else:
            flag(WARN, f"{sink}: {_n(value)} record(s) dropped {reason} before sending, counted")
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
    # An encode fault (a non-finite value, an oversized sketch, an unresolved GaugeDelta).
    dropped_metrics = _by_attr(sut, "logit.output.metrics.dropped", "reason", component=sink)
    dropped_metrics.pop("?", None)
    total_dropped = sum(sut.counter_by_life("logit.output.metrics.dropped",
                                            component=sink).values())
    if total_dropped:
        detail = ", ".join(f"{k} {_n(v)}" for k, v in sorted(dropped_metrics.items()))
        flag(WARN, f"{sink}: {_n(total_dropped)} metric(s) dropped by the encoder, never sent"
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
    if cued:
        lines.append(f"{sink}: cued by a respond step, excused: " + "; ".join(cued))
    return (f"{sink}: requests {shown or 'none'}; records accepted {_n(accepted)}; "
            + (f"cued, excused: {'; '.join(cued)}; " if cued else "") + "; ".join(identities))


def check_ledger_topic(data):
    """For a Kafka target: the topic's distinct record count T against the records `kafka_out`
    saw accepted, A (`logit.output.records`, summed over every SUT life), and the topic's
    duplicate copies per life, with, under `otlp_proto`, the data points a distinct record
    repeats. T below A FAILs, records the broker acknowledged and no longer
    holds, and T above A FAILs, records the sink never saw accepted, unless the sink dropped an
    attempt it never saw answered (an `ambiguous_at_most_once` record drop or any batch drop),
    which may have landed: a WARN. When a scheduled kill ended a SUT life, a T − A difference
    in either direction WARNs and names the killed lives: the kill loses that life's last
    telemetry interval of A, and a spooled batch counted before the kill and replayed after it
    is in A twice but stored once. A duplicate copy or a repeated point FAILs when the life
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
    lines = [f"topic {data.target.get('topic')!r} ({led.topic_format}): end offsets "
             f"{read.get('offsets')}, {_n(t_count + copies)} record(s) read back, "
             f"{_n(t_count)} distinct",
             "records accepted by life: " + ", ".join(
                 f"life {life} {_n(value)}" for life, value in sorted(by_life.items()))]
    status = PASS
    problems = []
    for life in sorted(set(led.duplicates) | set(led.point_copies)):
        count = led.duplicates.get(life, 0)
        points = led.point_copies.get(life, 0)
        what = " and ".join(part for part in (
            f"{_n(count)} duplicate copies" if count else "",
            f"{_n(points)} data point(s) a distinct record repeats" if points else "") if part)
        bumps = led.epoch_bumps(life)
        why = []
        if life != led.final:
            why.append("a later life resent under a new producer id")
        why += [f"{_n(value)} record(s) dropped {reason}" for reason, value in bumps.items()]
        if why:
            status = worst([status, WARN])
            problems.append(f"life {life}: {what} after an epoch bump "
                            f"({'; '.join(why)}), which at_least_once permits")
        else:
            status = FAIL
            problems.append(f"life {life}: {what} with no epoch bump: "
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


# What each fault class looks like at a sink (docs/adr/sink-fault-classes.md): `rejected` drops
# the batch at once under either posture and logs no `retrying` line; `clean`, `refused`, and
# `ambiguous` hold the head and log `retrying` with the class word, except `ambiguous` under
# `at_most_once`, which drops the batch `ambiguous_at_most_once`. `write_loop` logs `retrying` on a
# head's first failure and then at most once a minute, so a step's span holds its first line.
_RETRYING_CLASS_RE = re.compile(r"^send failed \((\w+)\)")


def _class_signature(data, led, sink, fault, at_most_once, slack):
    """`(ok, text)` for one classed step at `sink`: the `retrying` class words its span holds,
    and the batches the sink dropped from its start to `slack` after its end."""
    words = sorted({match.group(1) for entry in data.stderr.get(led.service, [])
                    if entry.kind == "json" and entry.key == "retrying"
                    and entry.component == sink and entry.ts is not None
                    and fault.start <= entry.ts <= fault.end
                    for match in [_RETRYING_CLASS_RE.match(entry.message)] if match})
    drops = {}
    for point in led.sut.matching("logit.component.batches.dropped", component=sink):
        if (isinstance(point.value, (int, float)) and point.value
                and fault.start < point.ts <= fault.end + slack):
            reason = point.attributes.get("reason", "?")
            drops[reason] = drops.get(reason, 0) + point.value
    want = fault.fault_class
    if want == "rejected":
        expected, ok = "a rejected drop and no retrying line", (
            drops.get("rejected", 0) > 0 and not words)
    elif want == "ambiguous" and at_most_once:
        expected, ok = "an ambiguous_at_most_once drop and no retrying line", (
            drops.get("ambiguous_at_most_once", 0) > 0 and not words)
    else:
        expected, ok = f"retrying ({want}) and no drop", words == [want] and not drops
    seen = (f"retrying {', '.join(f'({w})' for w in words) or 'none'}, dropped "
            f"{_reasons(drops)}")
    where = (f"{fault.step} ({fault.action} on {fault.on}"
             + (f", {fault.status}" if fault.status is not None else "")
             + (f", {fault.answer}" if fault.answer is not None else "")
             + (f", count {fault.count}" if fault.count is not None else "") + ")")
    return ok, f"{where}: {want}: {seen}" + ("" if ok else f"; expected {expected}")


def check_ledger_standin(data):
    """For a stand-in target, the record against the sink: every line parses and every body
    decodes; every sample of a write answered `2xx` was stored, which is what catches a black
    hole that a rejected step's drop credit would let `ledger.egress` count; no resend stored a
    different value for a sample already stored; per SUT life, the
    record's requests by status class equal the sink's `logit.output.requests` for 2xx to 5xx,
    and its resets are at most the sink's `network_error`; a refused or failed dial,
    `connect_error`, never reaches the record;
    every answer other than the default carries the cue of a `respond` step and falls inside
    its span, and every write went to the stand-in's path; each step with a `count` answered
    that many writes to the path under its cue; and each step with a `class` shows
    that class's effect at the sink (`_class_signature`). A killed life's mismatch WARNs, since
    a kill loses that life's last telemetry interval. Every other target SKIPs.

    Under the `datadog` shape (`_standin_datadog`), also: per SUT life, the sink's
    `records.dropped{reason="rejected"}` equals the series the `series_errors` answers named
    plus the points of the requests answered under the cue of a step classed `rejected`, other
    than a `413`, and `records.dropped{reason="oversize"}` equals the points of the `413`
    requests under such a step; and each duplicate copy of a stored point is reported: permitted
    under `at_least_once` after an answer the sink didn't see accepted, and a FAIL under
    `at_most_once`, or as a resend within one life of a point the stand-in answered `2xx`.

    Under the `influxdb` shape (`_standin_influxdb`), also: per SUT life, no point of a request
    answered under the cue of a step classed `rejected` is stored, and the egress gap, Ab − V
    plus a stopped earlier life's residual as `ledger.egress` adds it, equals those points'
    sum, credited to lives by timestamp; a killed life's is reported. The record's writes name
    one `(org, orgID, bucket)`, which the row reports. A point stored again with the same value
    is a rewrite InfluxDB absorbs, reported, and FAILs only under `at_most_once`, or as a
    resend within one life of a point the stand-in answered `2xx`."""
    if not data.standin:
        return Result("ledger.standin", SKIP, f"{data.target.get('kind')} target: no stand-in")
    if data.standin_text is None:
        return Result("ledger.standin", FAIL, "no stand-in record (logs/standin.stdout)")
    led = _ledger(data)
    skipped = _skip_without_sut("ledger.standin", led)
    if skipped:
        return skipped
    requests = data.standin_requests
    shape = data.standin_shape
    path = standin.PATHS[shape]
    status = PASS
    problems = []
    lines = []

    def flag(verdict, text):
        nonlocal status
        status = worst([status, verdict])
        problems.append((verdict, text))

    if not requests:
        flag(FAIL, "the stand-in recorded no request")
    if data.standin_unparsed:
        flag(FAIL, f"{_n(data.standin_unparsed)} stand-in record line(s) aren't JSON objects")
    _, rewrites, conflicts, _ = data.standin_store
    undecodable = []
    for request in requests:
        try:
            standin.decode_body(request, shape)
        except standin.DecodeError as err:
            undecodable.append(f"#{request.get('seq')}: {err}")
    if undecodable:
        flag(FAIL, f"{len(undecodable)} request body(ies) don't decode as "
                   f"{standin.BODY_NAMES[shape]}: "
                   f"{'; '.join(undecodable[:3])}")
    for key, stamp, first, last in conflicts[:5]:
        flag(FAIL, f"{dict(key).get('__name__', key)} at {stamp} ms stored {first:g}, then "
                   f"{last:g}: a resend changed a sample")
    if len(conflicts) > 5:
        flag(FAIL, f"{len(conflicts) - 5} more changed sample(s)")
    lost = standin.acknowledged_unstored(requests, data.standin_store[0], shape)
    if lost:
        key, stamp = lost[0]
        flag(FAIL, f"{_n(len(lost))} sample(s) the stand-in answered 2xx were never stored, "
                   f"first {dict(key).get('__name__', key)} at {data.offset(stamp / 1000)}: a "
                   "write the sink saw accepted and the destination lost")
    wrong_path = [r for r in requests if r.get("path") != path]
    if wrong_path:
        flag(FAIL, f"{len(wrong_path)} write(s) to a path other than {path}, first "
                   f"{wrong_path[0].get('path')!r}")

    sinks = led.sinks
    sink = sinks[0] if sinks else None
    by_life = {}
    for request in requests:
        life = vm.life_at(led.life_starts, request.get("t") or 0)
        cls = standin.status_class(request)
        by_life.setdefault(life, {})
        by_life[life][cls] = by_life[life].get(cls, 0) + 1
    for life in led.lives:
        recorded = by_life.get(life, {})
        counted = {}
        for point in led.sut.matching("logit.output.requests", component=sink):
            if point.life == life and isinstance(point.value, (int, float)):
                cls = point.attributes.get("class", "?")
                counted[cls] = counted.get(cls, 0) + point.value
        verdict = WARN if life in led.killed and life != led.final else FAIL
        unrecorded = {"network_error", HTTP_CONNECT_CLASS}
        for cls in sorted((set(recorded) | set(counted)) - unrecorded):
            if recorded.get(cls, 0) != counted.get(cls, 0):
                flag(verdict, f"life {life}: the stand-in answered {_n(recorded.get(cls, 0))} "
                              f"request(s) {cls}, {sink} counted {_n(counted.get(cls, 0))}")
        resets, errors = recorded.get("network_error", 0), counted.get("network_error", 0)
        if resets > errors:
            flag(verdict, f"life {life}: the stand-in reset {_n(resets)} request(s), {sink} "
                          f"counted {_n(errors)} network_error")
        lines.append(f"life {life}: recorded " + (", ".join(
            f"{cls} {_n(n)}" for cls, n in sorted(recorded.items())) or "none")
            + f"; {sink} counted " + (", ".join(
                f"{cls} {_n(n)}" for cls, n in sorted(counted.items())) or "none"))

    responds = {f.step: f for f in data.faults if f.action == "respond"}
    seen_cues = {}
    for request in requests:
        cue = request.get("cue")
        default = (request.get("answer") == "status"
                   and request.get("status") == standin.DEFAULT_STATUS[shape]
                   and request.get("stored") is True)
        if cue is None:
            if not default and request.get("path") == path:
                flag(FAIL, f"request #{request.get('seq')} was answered "
                           f"{request.get('answer')} {request.get('status')} with no cue")
            continue
        seen_cues[cue] = seen_cues.get(cue, 0) + 1
        fault = responds.get(cue)
        t = request.get("t") or 0
        if fault is None:
            flag(FAIL, f"request #{request.get('seq')} was answered under cue {cue}, which no "
                       "respond step applied cleanly")
        elif not fault.start - 1 <= t <= fault.end + 1:
            flag(FAIL, f"request #{request.get('seq')} was answered under cue {cue} at "
                       f"{data.offset(t)}, outside its span [{data.offset(fault.start)}, "
                       f"{data.offset(fault.end)}]")
    for step, fault in sorted(responds.items()):
        if not seen_cues.get(step) and fault.fault_class is None:
            flag(WARN, f"{step}: no request reached the stand-in under its cue")
        if fault.count is not None:
            # A counted cue answers that many writes and no more, so a shortfall means the
            # step's span closed before the sink sent them, and the step tested less than it says.
            spent = sum(1 for r in requests if r.get("cue") == step and r.get("path") == path)
            if spent != fault.count:
                flag(FAIL, f"{step}: its cue answered {_n(spent)} write(s), not its count "
                           f"{_n(fault.count)}")

    drains = led.sut.drains
    gaps = [b - a for a, b in zip(drains, drains[1:])]
    slack = 2 * (statistics.median(gaps) if gaps else 5.0)
    at_most_once = "at_most_once" in "".join(
        component_blocks(led._sut_config_text(led.service), sink or ""))
    classed = [f for f in data.faults if f.fault_class is not None]
    for fault in classed:
        ok, text = _class_signature(data, led, sink, fault, at_most_once, slack)
        lines.append(text)
        if not ok:
            flag(FAIL, text)

    stored = sum(1 for r in requests if r.get("stored"))
    if data.standin_datadog:
        resent = _standin_datadog(data, led, sink, at_most_once, flag, lines)
    elif data.standin_influxdb:
        resent = _standin_influxdb(data, led, sink, at_most_once, flag, lines)
    else:
        resent = f"{_n(rewrites)} resent sample(s) stored again with the same value"
    summary = (f"{_n(len(requests))} request(s) recorded, {_n(stored)} stored, "
               f"{_n(len(data.standin_cues))} cue(s); {resent}; {len(classed)} classed step(s) "
               f"({'at_most_once' if at_most_once else 'at_least_once'})")
    if status == PASS:
        return Result("ledger.standin", PASS, summary + "; record, request classes, cues, and "
                      "each step's class agree with the sink", lines)
    worst_first = sorted(problems, key=lambda item: -_ORDER[item[0]])
    return Result("ledger.standin", status, f"{worst_first[0][1]}; {summary}",
                  [text for _, text in worst_first] + lines)


def _standin_datadog(data, led, sink, at_most_once, flag, lines):
    """`ledger.standin`'s rules for a `datadog` stand-in (`check_ledger_standin`'s docstring):
    flags each problem through `flag`, appends its lines, and returns the summary's duplicate
    clause."""
    requests = data.standin_requests
    counts = data.standin_counts
    classed = {f.step: f for f in data.faults if f.action == "respond" and f.fault_class}

    # The record's drops per life, by the request's time, against the sink's.
    expected = {}
    for seq, named, t in counts.named:
        life = vm.life_at(led.life_starts, t or 0)
        expected.setdefault(life, {"rejected": 0, "oversize": 0})["rejected"] += named
    for request in requests:
        # The step's `class` is the only statement of what the sink should read; the default
        # 2xx and a `series_errors` 2xx carry none.
        fault = classed.get(request.get("cue"))
        if fault is None or fault.fault_class != "rejected":
            continue
        reason = "oversize" if request.get("status") == 413 else "rejected"
        life = vm.life_at(led.life_starts, request.get("t") or 0)
        expected.setdefault(life, {"rejected": 0, "oversize": 0})[reason] += (
            standin.points_in(request, standin.DATADOG))
    for life in led.lives:
        verdict = WARN if life in led.killed and life != led.final else FAIL
        want = expected.get(life, {"rejected": 0, "oversize": 0})
        for reason in ("rejected", "oversize"):
            got = sum(value for _, value, point_life in led.sut.counter_points(
                "logit.output.records.dropped", component=sink, reason=reason)
                if point_life == life)
            if got != want[reason]:
                what = ("the series series_errors answers named plus the points of requests "
                        "answered under a rejected step" if reason == "rejected" else
                        "the points of requests answered 413 under a rejected step")
                flag(verdict, f"life {life}: {sink} counted {_n(got)} record(s) dropped "
                              f"{reason}, the record holds {_n(want[reason])}: {what}")
        lines.append(f"life {life}: records dropped rejected {_n(want['rejected'])}, oversize "
                     f"{_n(want['oversize'])} by the record")

    copies = {}
    for key, stamp, value, first_at, copy_at in counts.duplicates:
        life = vm.life_at(led.life_starts, stamp / 1000)
        one = copies.setdefault(life, {"copies": 0, "amount": 0.0})
        one["copies"] += 1
        one["amount"] += value
        first, again = requests[first_at], requests[copy_at]
        # `seq` starts again when the stand-in restarts, so the record position names them too.
        first_seq = f"#{first.get('seq')} (record request {first_at + 1})"
        copy_seq = f"#{again.get('seq')} (record request {copy_at + 1})"
        same_life = (vm.life_at(led.life_starts, first.get("t") or 0)
                     == vm.life_at(led.life_starts, again.get("t") or 0))
        name = dict(key).get("__name__", key)
        if standin.status_class(first) == "2xx" and same_life:
            flag(FAIL, f"request {copy_seq} stored {name} at {stamp // 1000} s again, which "
                       f"request {first_seq} stored under a 2xx: the sink resent a point it "
                       "saw accepted")
        elif at_most_once:
            flag(FAIL, f"request {copy_seq} stored {name} at {stamp // 1000} s again, first "
                       f"stored by request {first_seq}: a resend under at_most_once")
    for life, one in sorted(copies.items()):
        lines.append(f"life {life}: {_n(one['copies'])} duplicate copies of stored points, "
                     f"{_n(one['amount'])} counted again by the destination"
                     + (", which at_least_once permits" if not at_most_once else ""))
    total = sum(one["copies"] for one in copies.values())
    amount = sum(one["amount"] for one in copies.values())
    per_life = ", ".join(f"life {life} {_n(one['copies'])}"
                         for life, one in sorted(copies.items()))
    return (f"{_n(total)} duplicate copies of stored points ({per_life or 'none'}), "
            f"{_n(amount)} counted twice by the destination, reported; "
            f"{_n(sum(named for _, named, _ in counts.named))} series named by series_errors")


def _standin_influxdb(data, led, sink, at_most_once, flag, lines):
    """`ledger.standin`'s rules for an `influxdb` stand-in (`check_ledger_standin`'s
    docstring): flags each problem through `flag`, appends its lines, and returns the summary's
    rewrite clause."""
    requests = data.standin_requests
    counts = data.standin_counts
    classed = {f.step: f for f in data.faults if f.action == "respond" and f.fault_class}
    stored = {(standin.series_key(one["metric"]), stamp) for one in counts.series
              for stamp in one["timestamps"]}

    # Every point a rejected step's answer refused: never stored, and the life's whole gap.
    rejected = {}
    kept = []
    for request in requests:
        fault = classed.get(request.get("cue"))
        if fault is None or fault.fault_class != "rejected":
            continue
        try:
            decoded = standin.decode_body(request, standin.INFLUXDB)
        except standin.DecodeError:
            continue
        for labels, samples in decoded:
            key = standin.series_key(labels)
            for stamp, value in samples:
                life = vm.life_at(led.life_starts, stamp / 1000)
                rejected[life] = rejected.get(life, 0.0) + value
                if (key, stamp) in stored:
                    kept.append((request, key, stamp))
    for request, key, stamp in kept[:5]:
        flag(FAIL, f"{dict(key).get('__name__', key)} at {data.offset(stamp / 1000)} is "
                   f"stored, though request #{request.get('seq')} under {request.get('cue')}, "
                   "classed rejected, carried it: the sink resent a rejected batch")
    if len(kept) > 5:
        flag(FAIL, f"{len(kept) - 5} more stored point(s) of rejected requests")
    for life in led.lives:
        want = rejected.get(life, 0.0)
        gap = led.absorbed(life) - led.v(life, [sink])
        if life != led.final:
            gap += led.residual(life)
        text = (f"life {life}: egress gap {_n(gap)}, the rejected requests' points sum "
                f"{_n(want)}")
        if life in led.killed and life != led.final:
            lines.append(text + ", reported: a killed life's gap is judged in its band")
        elif not math.isclose(gap, want, abs_tol=1e-6):
            flag(FAIL, text + ": a gap the rejected steps don't explain")
        else:
            lines.append(text)

    rewritten = {}
    for key, stamp, value, first_at, copy_at in counts.rewrites:
        life = vm.life_at(led.life_starts, stamp / 1000)
        rewritten[life] = rewritten.get(life, 0) + 1
        first, again = requests[first_at], requests[copy_at]
        first_seq = f"#{first.get('seq')} (record request {first_at + 1})"
        copy_seq = f"#{again.get('seq')} (record request {copy_at + 1})"
        same_life = (vm.life_at(led.life_starts, first.get("t") or 0)
                     == vm.life_at(led.life_starts, again.get("t") or 0))
        name = dict(key).get("__name__", key)
        if standin.status_class(first) == "2xx" and same_life:
            flag(FAIL, f"request {copy_seq} wrote {name} at {stamp} ms again, which request "
                       f"{first_seq} stored under a 2xx: the sink resent a point it saw "
                       "accepted")
        elif at_most_once:
            flag(FAIL, f"request {copy_seq} wrote {name} at {stamp} ms again, first stored by "
                       f"request {first_seq}: a resend under at_most_once")
    for life, n in sorted(rewritten.items()):
        lines.append(f"life {life}: {_n(n)} point(s) stored again with the same value, which "
                     "InfluxDB overwrites")

    destinations = {}
    for request in requests:
        if request.get("path") == standin.INFLUXDB_WRITE_PATH:
            one = standin.influxdb_destination(request)
            destinations[one] = destinations.get(one, 0) + 1

    def named(one):
        return ", ".join(f"{name}={value!r}" for name, value in zip(("org", "orgID", "bucket"),
                                                                    one) if value is not None)

    shown = "; ".join(f"{named(one) or 'none'} ({_n(n)} write(s))"
                      for one, n in sorted(destinations.items(), key=lambda item: -item[1]))
    if len(destinations) > 1:
        flag(FAIL, f"the writes name {len(destinations)} destinations, one expected: {shown}")
    per_life = ", ".join(f"life {life} {_n(n)}" for life, n in sorted(rewritten.items()))
    return (f"{_n(len(counts.rewrites))} point(s) rewritten with the same value "
            f"({per_life or 'none'}), reported; "
            f"{_n(sum(rejected.values()))} in rejected requests' points; destination "
            f"{shown or 'none'}")


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
    `buffer.batches` at most 1, every UDP listener's `receive.utilization` is under 0.05, and the
    ingest rate summed over the listeners over the drain interval ending there is at least 95%
    of the generators' summed warmup rate. A drain interval another fault overlaps, or that
    spans two SUT lives, isn't eligible; a fault with no eligible interval is skipped and
    listed. A `rate_behind` diagnostic from any generator turns a throughput shortfall into a
    WARN. Across a hop each SUT's sinks are judged on its own drains: the hop sink against the
    listeners' W, and the far SUT's sinks against its `logit_in`'s events sent. Under
    `[overlap]` each instance's sinks and receive queue are judged on its own drains, so only
    while it runs, against the ingest summed over every instance (`_life_cumulative`), since the
    generators' flows move between them."""
    leds = None
    if _overlapped(data):
        leds = _instance_ledgers(data)
        problems = [problem for one in leds.values() for problem in one.problems]
        if problems:
            return Result("recovery", SKIP, "; ".join(problems))
        led = leds[data.instances[0]]
    else:
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
        gen = led.gens[source.get("service")]
        for load in source_ids(source, "generator_input"):
            warm = [(ts, v) for ts, v, _ in gen.counter_points("logit.component.events.sent",
                                                               component=load)
                    if data.t0 <= ts <= data.t0 + data.warmup]
            if len(warm) < 2 or warm[-1][0] <= warm[0][0]:
                return Result("recovery", SKIP, f"too few {source.get('service')} drains in "
                              "warmup for a baseline")
            baseline += sum(v for _, v in warm[1:]) / (warm[-1][0] - warm[0][0])
            behind += sum(v for _, v, _ in gen.counter_points(
                "logit.component.diagnostics", component=load, key="rate_behind"))
    # Only a UDP listener has a receive queue to drain.
    receive_util = {listener: led.ingress.gauge_series("logit.component.receive.utilization",
                                                       component=listener)
                    for listener in led.listeners if led.wires[listener] is WIRES["udp"]}

    def leg(tel, sinks, fed, label=""):
        """One SUT's sinks, each judged on that SUT's own drains against what reached it per
        drain, `fed` `[(component, counter)]`: the listeners' W counters, or across a hop for
        the far SUT its `logit_in`'s events sent."""
        ingest = {}
        for component, name in fed:
            for ts, v, _ in tel.counter_points(name, component=component):
                ingest[ts] = ingest.get(ts, 0) + v
        life_of = {p.ts: p.life for p in tel.points if p.name == telemetry.UPTIME}
        return {"sinks": sinks, "label": label, "ingest": ingest, "ingest_ts": sorted(ingest),
                "life_of": life_of, "drains": sorted(life_of),
                "gauges": {sink: {name: tel.gauge_series(f"logit.component.{name}",
                                                         component=sink)
                                  for name in ("retrying", "buffer.utilization",
                                               "buffer.batches")} for sink in sinks}}

    datagrams = [(listener, led.wires[listener].w) for listener in led.listeners]
    if leds is not None:
        totals = [_life_cumulative(one.ingress, [(name, {"component": listener})
                                                 for listener, name in datagrams])
                  for one in leds.values()]

        def summed(start, end):
            return sum(f(end) - f(start) for f in totals)

        legs = []
        for instance, one in leds.items():
            legs.append({**leg(one.ingress, one.sinks, datagrams, f"{instance} "),
                         "ingested": summed,
                         "receive": {listener: one.ingress.gauge_series(
                             "logit.component.receive.utilization", component=listener)
                             for listener in receive_util}})
    elif led.hop is None:
        legs = [leg(sut, led.sinks, datagrams)]
    else:
        legs = [leg(led.ingress, [led.hop.get("sink")], datagrams, f"{led.inn.service} "),
                leg(sut, led.sinks, [(led.hop.get("listener"), "logit.component.events.sent")],
                    f"{led.service} ")]

    def ingested(one, start, end):
        """What reached `one`'s SUT in its drains in (start, end]: under `[overlap]`, every
        instance."""
        if "ingested" in one:
            return one["ingested"](start, end)
        lo = bisect.bisect_right(one["ingest_ts"], start)
        hi = bisect.bisect_right(one["ingest_ts"], end)
        return sum(one["ingest"][ts] for ts in one["ingest_ts"][lo:hi])

    def assess(one, sink, start, end):
        """(problems, rate problem or None, rate, share) for one sink over the drain interval
        (start, end]."""
        problems = []
        retrying = one["gauges"][sink]["retrying"]
        buffer_batches = one["gauges"][sink]["buffer.batches"]
        if _step_value(retrying, end) != 0:
            problems.append(f"retrying {_step_value(retrying, end):g}")
        receive = [(f"{listener} receive.utilization" if len(receive_util) > 1
                    else "receive.utilization", series)
                   for listener, series in one.get("receive", receive_util).items()]
        for label, series in ([("buffer.utilization", one["gauges"][sink]["buffer.utilization"])]
                              + receive):
            value = _step_value(series, end)
            # One batch in a small queue is a large share of it: a batch the sink holds
            # while it sends is recovered at any `max_batches`, unless that batch fills it.
            if (label == "buffer.utilization" and value < 1.0
                    and _step_value(buffer_batches, end) <= 1):
                continue
            if value >= RECOVERED_UTILIZATION:
                problems.append(f"{label} {value:.3f}")
        rate = ingested(one, start, end) / (end - start)
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
        any_eligible = False
        for one in legs:
            drains, life_of = one["drains"], one["life_of"]
            eligible = [(a, b) for a, b in zip(drains, drains[1:])
                        if a >= fault.end and b <= bound and b < data.end
                        and life_of[a] == life_of[b]
                        and not any(f is not fault and f.start < b and f.end > a
                                    for f in data.faults)]
            if not eligible:
                lines.append(f"{name}: {one['label']}skipped, no drain interval within "
                             f"{data.recovery_bound:g}s of its end clear of other faults")
                continue
            any_eligible = True
            for sink in one["sinks"]:
                last = None
                for a, b in eligible:
                    problems, slow, rate, share = assess(one, sink, a, b)
                    if not problems and slow is None:
                        lines.append(f"{name}: {one['label']}{sink} recovered at "
                                     f"{data.offset(b)}, {b - fault.end:.0f}s after its end, "
                                     f"ingest {rate:,.0f}/s ({share:.0%})")
                        break
                    last = (b, problems, slow)
                else:
                    b, problems, slow = last
                    verdict = FAIL if problems else (WARN if behind > 0 else FAIL)
                    if slow is not None:
                        problems.append(slow + (f", but a generator logged rate_behind "
                                                f"{_n(behind)} time(s)" if behind > 0 else ""))
                    status = worst([status, verdict])
                    lines.append(f"{name}: {one['label']}{sink} not recovered by "
                                 f"{data.offset(b)}: {'; '.join(problems)}")
        judged += any_eligible
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


# ---- file reopens ---------------------------------------------------------------------------------
#
# A `rename` moves a `stdio_out` file target aside in `<run>/shared`, and a SIGHUP inside the step
# makes the target reopen its path. The reopen is lazy: the target checks the reopen generation at
# the top of each send (crates/logit-outputs/src/stdio.rs, "Reopen on SIGHUP"), so the first batch
# after the signal opens the new file and every earlier batch is in the renamed one.

# How far a line's timestamp may sit past the signal's end in the renamed file, or before its
# start in the new one. A line's timestamp is when its listener read the datagram (statsd_in's
# `received_at`), and stdio_out writes each batch as it arrives: statsd_in sends a batch at least
# every 100 ms (`batch_flush_interval`), so a line reaches the file about 100 ms after it was
# read. 2 s is 20 of those, for a loaded host, and far under the minutes between a scenario's
# signals, so a target that missed the reopen is off by far more.
REOPEN_TOLERANCE_S = 2.0
# What stands in for a run's `shared/` files once they are deleted to save space:
# `{file name: ndjson_file_lines(file)}`, read for a name `shared/` no longer holds.
SHARED_COUNTS = "shared-counts.json"
_NDJSON_TIMESTAMP = re.compile(rb'^\{"timestamp":"([^"]+)"')


def _service_config_text(data, service):
    """The config text of `logit` service `service` from the run's `configs/` copy, or ""."""
    configs = data.resolved.get("configs") or {}
    key = (service if service in configs and service != SUT_CONFIG_KEY
           else SUT_CONFIG_KEY if service in data.suts else None)
    if key is None or key not in configs:
        return ""
    try:
        return (data.run_dir / "configs" / configs[key]).read_text()
    except OSError:
        return ""


def ndjson_file_lines(path):
    """`{lines, first, last, unparsed}` for a `format: json` file: its newline-terminated lines,
    the earliest and latest `timestamp` among them in epoch seconds (None with no lines), and how
    many lines carry none. None when the file doesn't exist. Each whole second is parsed once,
    since a file holds thousands of lines a second."""
    path = Path(path)
    if not path.is_file():
        return None
    lines = unparsed = 0
    first = last = None
    seconds = {}
    with open(path, "rb") as handle:
        for raw in handle:
            if not raw.endswith(b"\n"):
                continue
            lines += 1
            found = _NDJSON_TIMESTAMP.match(raw)
            text = found.group(1).decode("ascii", "replace") if found else ""
            whole, dot, rest = text.partition(".")
            try:
                if dot and rest.endswith("Z") and rest[:-1].isdigit():
                    if whole not in seconds:
                        seconds[whole] = telemetry.parse_rfc3339(whole + "Z")
                    ts = seconds[whole] + int(rest[:-1]) / 10 ** len(rest[:-1])
                else:
                    ts = telemetry.parse_rfc3339(text)
            except ValueError:
                unparsed += 1
                continue
            first = ts if first is None or ts < first else first
            last = ts if last is None or ts > last else last
    return {"lines": lines, "first": first, "last": last, "unparsed": unparsed}


def _reopen_files(data, names):
    """`{name: ndjson_file_lines()}` for each of `names` in the run's `shared/`, taking a name
    the directory no longer holds from `SHARED_COUNTS` when the run has one."""
    try:
        recorded = json.loads((data.run_dir / SHARED_COUNTS).read_text())
    except (OSError, ValueError):
        recorded = {}
    files = {}
    for name in names:
        found = ndjson_file_lines(data.run_dir / SHARED_DIR / name)
        files[name] = found if found is not None else recorded.get(name)
    return files


def _reopen_count(data, service, path, names, files):
    """`(status, line)` for the lines in every file of `path`'s renames against the target's
    `events.delivered` summed over `service`'s lives."""
    component = stdio_shared_targets(_service_config_text(data, service)).get(path)
    written = sum(found["lines"] for found in files.values() if found)
    tel = data.telemetry.get(service)
    shown = f"lines in {', '.join(names)}: {_n(written)}"
    if component is None:
        return FAIL, f"{shown}; {service}'s config has no stdio_out at {SHARED_MOUNT}/{path}"
    if tel is None:
        return FAIL, f"{shown}; no {service} telemetry"
    delivered = sum(tel.counter_by_life("logit.component.events.delivered",
                                        component=component).values())
    line = f"{shown}, {component}'s events.delivered over {service}'s lives: {_n(delivered)}"
    if written == delivered:
        return PASS, line
    ended = [f.step for f in data.faults if f.on == service and f.action in ("kill", "exit")]
    if ended:
        return PASS, (f"{line} (reported, not judged: {', '.join(ended)} ended a life with no "
                      "final drain)")
    return FAIL, f"{line}, off by {_n(written - delivered)}"


def check_file_reopen(data):
    """One `file.reopen.<step>` row per `rename` step, else one `file.reopen` row that SKIPs.
    A rename moves its target's file `path` to `to` and pairs with the first SIGHUP on its
    service starting inside its window. The row FAILs when the rename or that SIGHUP never
    applied; the renamed file is missing or has a line timestamped after the signal's apply
    finished plus `REOPEN_TOLERANCE_S` (or, after an earlier rename of the same path, before that
    rename's signal started less it); the new file, the next rename's `to` or `path` after the
    last, is missing or has a line timestamped before the signal started less it; a line has no
    timestamp; or the lines in every file of the path's renames differ from the target's
    `events.delivered` summed over the service's lives, one line per event under
    `format: json`. A kill or an `exit` of the service loses its last drain's counts, so the
    count is then reported, not judged. A new file with no lines WARNs."""
    renames = sorted((f for f in data.events if f.action == "rename"), key=lambda f: f.start)
    failed = [r for r in data.timeline if r.get("event") == "apply"
              and r.get("action") == "rename" and r.get("rc")]
    if not renames and not failed:
        return [Result("file.reopen", SKIP, "the scenario has no rename step")]
    results = [Result(f"file.reopen.{r.get('step')}", FAIL,
                      f"the rename of {r.get('path')} to {r.get('to')} on {r.get('on')} "
                      f"returned rc {r.get('rc')}: {(r.get('stderr') or '')[:160]}")
               for r in failed]
    hups = sorted((f for f in data.events if f.action == "signal" and f.signal == "HUP"),
                  key=lambda f: f.start)
    chains = {}
    for rename in renames:
        chains.setdefault((rename.on, rename.path), []).append(rename)
    for (service, path), chain in chains.items():
        names = [rename.to for rename in chain] + [path]
        files = _reopen_files(data, names)
        count = _reopen_count(data, service, path, names, files)
        previous = None
        for index, rename in enumerate(chain):
            hup = next((f for f in hups if f.on == service
                        and rename.start <= f.start <= rename.end), None)
            new_name = names[index + 1]
            old, new = files[rename.to], files[new_name]
            status, problems, lines = count[0], [], [count[1]]
            if count[0] == FAIL:
                problems.append(count[1])
            if hup is None:
                problems.append(f"no SIGHUP on {service} applied inside {rename.step}")
            if old is None:
                problems.append(f"{rename.to} is missing")
            elif hup is not None:
                lines.append(f"{rename.to}: {_n(old['lines'])} line(s), the latest at "
                             f"{data.offset(old['last'])}; {hup.step}'s SIGHUP finished at "
                             f"{data.offset(hup.applied)}")
                if old["last"] is not None and old["last"] > hup.applied + REOPEN_TOLERANCE_S:
                    problems.append(f"{rename.to} has a line at {data.offset(old['last'])}, "
                                    f"{old['last'] - hup.applied:.1f}s after {hup.step}'s "
                                    f"SIGHUP finished, past the {REOPEN_TOLERANCE_S:g}s "
                                    "tolerance")
                if (previous is not None and old["first"] is not None
                        and old["first"] < previous.start - REOPEN_TOLERANCE_S):
                    problems.append(f"{rename.to} has a line at {data.offset(old['first'])}, "
                                    f"{previous.start - old['first']:.1f}s before "
                                    f"{previous.step}'s SIGHUP started")
            if new is None:
                problems.append(f"the new file {new_name} is missing: the target never reopened "
                                "its path")
            elif hup is not None:
                lines.append(f"{new_name}: {_n(new['lines'])} line(s), the earliest at "
                             f"{data.offset(new['first'])}; {hup.step}'s SIGHUP started at "
                             f"{data.offset(hup.start)}")
                if new["first"] is None:
                    status = worst([status, WARN])
                    lines.insert(0, f"the new file {new_name} has no lines")
                elif new["first"] < hup.start - REOPEN_TOLERANCE_S:
                    problems.append(f"{new_name} has a line at {data.offset(new['first'])}, "
                                    f"{hup.start - new['first']:.1f}s before {hup.step}'s "
                                    f"SIGHUP started, past the {REOPEN_TOLERANCE_S:g}s "
                                    "tolerance")
            for name, found in ((rename.to, old), (new_name, new)):
                if found and found["unparsed"]:
                    problems.append(f"{name}: {_n(found['unparsed'])} line(s) with no "
                                    "timestamp")
            previous = hup
            if problems:
                status = FAIL
            who = f"{hup.step}'s" if hup else "no"
            head = f"{service}: {path} -> {rename.to} with {who} SIGHUP"
            shown = problems or lines
            results.append(Result(f"file.reopen.{rename.step}", status,
                                  f"{head}; {'; '.join(shown)}", problems + lines))
    return results


# ---- served certificates --------------------------------------------------------------------------
#
# A `rotate` issues a certificate's next generation into `<run>/tls` and repoints its `live`
# symlink; a SIGHUP makes the listener reload the files, and a `probe` reads what the listener
# serves after it. A failed reload keeps the previous files, so after a broken rotation the
# listener still serves the last good generation (docs/adr/tls-certificate-reload.md, "Failure
# policy: keep serving the old material").


def check_tls_served(data):
    """One `tls.served.<step>` row per `probe` step, else one `tls.served` row that SKIPs. The
    probe pairs with the generation in force for its certificate: the latest rotation of it
    that started before the probe and wasn't broken, else the first issue (the driver's
    `tls_issued` phase). The row FAILs when the probe didn't apply, the served serial or
    `notAfter` isn't that generation's, the served chain didn't verify against the run's CA for
    the certificate's name, or no first issue was recorded."""
    probes = sorted((f for f in data.events if f.action == "probe"), key=lambda f: f.start)
    failed = [r for r in data.timeline if r.get("event") == "apply"
              and r.get("action") == "probe" and r.get("rc")]
    if not probes and not failed:
        return [Result("tls.served", SKIP, "the scenario has no probe step")]
    results = [Result(f"tls.served.{r.get('step')}", FAIL,
                      f"the probe of {r.get('cert')} on {r.get('on')} returned rc "
                      f"{r.get('rc')}: {(r.get('stderr') or '')[:160]}")
               for r in failed]
    issued = (data.phases.get("tls_issued") or {}).get("certs") or {}
    rotations = sorted((f for f in data.events if f.action == "rotate"), key=lambda f: f.start)
    for probe in probes:
        first = issued.get(probe.cert)
        want = None if first is None else {"what": "the first issue (gen 1)",
                                           "serial": first.get("serial"),
                                           "not_after": first.get("not_after")}
        skipped = []
        for rotation in rotations:
            if rotation.cert != probe.cert or rotation.start > probe.start:
                continue
            if rotation.broken:
                skipped.append(rotation.step)
                continue
            want = {"what": f"{rotation.step}'s gen {rotation.gen}", "serial": rotation.serial,
                    "not_after": rotation.not_after}
        served = (f"{probe.on}:{probe.port} served {probe.cert} serial {probe.serial}, "
                  f"notAfter {probe.not_after}")
        if want is None:
            results.append(Result(f"tls.served.{probe.step}", FAIL,
                                  f"{served}; no first issue of {probe.cert} recorded "
                                  "(tls_issued)"))
            continue
        line = (f"{served}; want {want['what']}, serial {want['serial']}, notAfter "
                f"{want['not_after']}"
                + (f", still in force after broken {', '.join(skipped)}" if skipped else ""))
        problems = []
        if probe.serial != want["serial"]:
            problems.append("serial differs")
        if probe.not_after != want["not_after"]:
            problems.append("notAfter differs")
        if probe.verified is not True:
            problems.append("the served chain didn't verify against the run's CA")
        status = FAIL if problems else PASS
        results.append(Result(f"tls.served.{probe.step}", status,
                              f"{line}" + (f": {'; '.join(problems)}" if problems else "")))
    return results


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
    sums them, `rate` is that sum per hour of the window's length, and `min_delta` is the
    smallest single drain's delta, a drain with no point counting 0, since a loss counter isn't
    emitted when it's zero. `value` is None when no drain falls in the window."""
    per_drain = {ts: value for ts, value, _ in tel.counter_points(metric, **attrs)}
    drains = [d for d in sorted(set(tel.drains) | set(per_drain)) if start < d <= end]
    if not drains:
        return None, "no drain in the window"
    values = [per_drain.get(d, 0) for d in drains]
    if reduce == "delta":
        return sum(values), f"over {len(drains)} drain(s)"
    if reduce == "rate":
        if end <= start:
            return None, "the window has no length"
        hours = (end - start) / SECONDS_PER_HOUR
        return (sum(values) / hours,
                f"{_n(sum(values))} over {end - start:.0f}s, {len(drains)} drain(s), per hour")
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
    set reads as unset in the next. `last` is the value in force at `end`. `rise` is that value
    less the value in force at `start`, and None with no value in force there. `value` is None
    when the gauge was never set by `end`.

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
    if reduce == "rise":
        if not before:
            return None, f"no value in force at the window's start ({len(inside)} sample(s))"
        return values[-1] - before[-1], note
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
    faults = [f for f in data.faults + data.events if f.step in scheduled]
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


def _balanced_drop(led, service, name, component, reason, batch_drops):
    """Whether a row that runs already judges this `*.dropped` term of SUT `service`'s
    `component`, so `drops` leaves it out. Each term is judged only in the SUT it belongs to:
    the listener terms in the listeners' SUT, the rest of the ledger's in the sinks' SUT, and
    across a hop the hop sink's drops by `ledger.hop`, and `logit_in`'s X_B in the far SUT's
    `fed`. With one SUT both are the same."""
    sinks_side = service == led.service
    hop_sink = led.hop is not None and service == led.inn.service and component == led.hop.get(
        "sink")
    # Every node's `batches.dropped` in the sinks' SUT is judged by `ledger.egress`,
    # `ledger.summary`, and `identity.sink` (a torn tail included), and the hop sink's by
    # `ledger.hop`; a node's `events.dropped` beside a batch drop of the same reason counts the
    # same loss. `bytes.dropped` counts `datagrams.dropped`'s datagrams again. On an external
    # target `ledger.egress` SKIPs and `ledger.summary` reads no fault drop, so a
    # `FAULT_DROP_REASONS` batch drop at a node other than a sink stays here; `ledger.sent`
    # FAILs one at a sink.
    if name == "logit.component.batches.dropped":
        if hop_sink:
            return True
        return sinks_side and not (led.data.external and reason in FAULT_DROP_REASONS
                                   and component not in led.sinks)
    if name == "logit.component.bytes.dropped":
        return True
    if (name == "logit.component.events.dropped" and (component, reason) in batch_drops
            and _balanced_drop(led, service, "logit.component.batches.dropped", component,
                               reason, batch_drops)):
        return True
    if hop_sink and name == "logit.component.events.dropped":
        return True  # `ledger.hop`'s counted hop drops
    if (sinks_side and led.hop is not None and component == led.hop.get("listener")
            and name == "logit.component.events.dropped"):
        return True  # X_B
    if service == led.inn.service and component in led.listeners:
        if name in ("logit.component.datagrams.dropped", "logit.input.kernel.dropped",
                    "logit.component.events.dropped"):
            return True  # D, K, and X
        wire = led.wires[component]
        if (name, reason) == ("logit.input.messages.dropped", "malformed") or (
                name == "logit.input.frames.dropped" and reason in wire.b_frames):
            return True  # B
        if name == "logit.input.frames.dropped" and reason in wire.counted:
            return True  # F, which `ledger.wire` judges
    if (sinks_side and component == led.script and name == "logit.component.events.dropped"
            and reason == "script_error"):
        return True  # L_err
    if (sinks_side and led.targeted and name == "logit.component.events.dropped"
            and component in led.targets.values()):
        return True  # T_x, in `ledger.edge`'s branch and a stopped life's egress gap
    if sinks_side and component in led.sinks and (led.data.external or led.data.kafka
                                                  or led.data.standin_datadog) and name in (
            "logit.output.records.dropped", "logit.output.metrics.dropped"):
        return True  # `ledger.sent` judges these on an external, Kafka, or datadog stand-in target
    return False


def check_drops(data):
    """Every `logit.*.dropped` counter a SUT service emitted that no other row judges: loss by
    the name alone (ADR `telemetry-loss-vocabulary`). A `FAULT_DROP_REASONS` reason FAILs, any
    other WARNs. Terms the ledger and the sink rows balance stay with them. With several SUT
    services each line names its service."""
    if _overlapped(data):
        return _per_instance(data, check_drops)
    led = _ledger(data)
    skipped = _skip_without_sut("drops", led)
    if skipped:
        return skipped
    totals = {}
    for service, view in led.views.items():
        tel = view.tel
        batch_drops = {(point.attributes.get("component"), point.attributes.get("reason"))
                       for point in tel.matching("logit.component.batches.dropped")
                       if isinstance(point.value, (int, float)) and point.value}
        names = sorted({point.name for point in tel.points
                        if point.name.startswith("logit.") and point.name.endswith(".dropped")})
        for name in names:
            for point in tel.matching(name):
                if not isinstance(point.value, (int, float)) or not point.value:
                    continue
                component = point.attributes.get("component", "?")
                reason = point.attributes.get("reason", "?")
                if _balanced_drop(led, service, name, component, reason, batch_drops):
                    continue
                key = (name, service, component, reason)
                totals[key] = totals.get(key, 0) + point.value
    if not totals:
        return Result("drops", PASS, "no *.dropped count outside the ledger and the sink rows")
    status = PASS
    lines = []
    for (name, service, component, reason), value in sorted(totals.items()):
        verdict = FAIL if reason in FAULT_DROP_REASONS else WARN
        status = worst([status, verdict])
        label = f"{service} " if len(led.views) > 1 else ""
        lines.append(f"{label}{component}: {name}{{reason=\"{reason}\"}} {_n(value)}")
    return Result("drops", status, f"{len(lines)} *.dropped count(s) outside the ledger: "
                  + "; ".join(lines), lines)


CHECKS = (
    check_run, check_timeline, check_exit, check_restarts, check_self_log, check_ready,
    check_progress, check_rss_slope, check_fd_slope, check_ledger_wire, check_ledger_overlap,
    check_ledger_intake, check_ledger_edge, check_ledger_hop, check_ledger_script, check_ledger_aggregate,
    check_ledger_egress, check_ledger_windows, check_ledger_replay, check_ledger_summary,
    check_ledger_sent, check_ledger_topic, check_ledger_standin, check_identity_sink, check_drops, check_recovery,
    check_file_reopen, check_tls_served, check_expect,
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
