"""Runs one scenario: brings the stack up, applies the fault schedule on a 1 s tick while the
watchdog polls, runs the end sequence, collects, tears down, and scores.

docs/plans/soak-harness.md's "The driver loop and end sequence" is the spec. Facts a maintainer
needs at the code:

- The loop is single-threaded. A slow action (a `docker stop` waits up to its `-t`) delays the
  tick, and the timeline records when each action started and finished, not only when it was
  planned.
- Every deadline is absolute, from the timeline's zero: the tick, each poll's next multiple of
  its period, and each step's offset. A late tick or a slow poll delays what follows it and
  never shifts a later deadline, so nothing drifts over hours.
- Every subprocess has a timeout (docker.py); none can hang the loop. The polls use shorter
  ones than actions do, and a poll round stops at its first timeout, so a hung daemon costs
  each poll one timeout per round rather than one per container.
- Logs are captured in chunks every `LOG_CHUNK_EVERY_S` (collect.py's `LogCapture` has the
  boundary rules), and collection at the end fetches only the tail.
- A `logit` container leaving `running`, or changing `StartedAt`, with no step behind it ends the
  schedule at once (fail fast). The end sequence, collection, and scoring still run. A tick more
  than `STALL_S` late, as after a host suspend, ends the schedule the same way, before any
  further step: the run can't be judged, so the loop records `stalled` and `aborted`, and the
  script exits `STALLED_EXIT` so a caller knows to rerun it. Inside an
  `exit` step the container may be `running` or `exited`; its revert starts it only if it
  exited, and only then is it a start, with a readiness probe.
- `docker exec` into a paused container blocks, so readiness is never probed while a step holds
  the container paused.
- The `try`/`finally` around the run collects logs and tears the project down on every exit
  but SIGKILL, unless `--keep`. SIGTERM and SIGHUP raise `KeyboardInterrupt` as SIGINT does;
  Python's default for both ends the process without running `finally`. A signal inherited as
  ignored stays ignored, so `nohup` keeps a run alive past a closed terminal.
- After a SIGHUP the terminal can be gone, so output goes through `_print`, which drops it
  rather than fail the run: the run directory holds everything.
- `prepare()` resolves the stack's services by role from `docker compose config`, which reads
  the base compose.yaml and the scenario's override with no daemon call, and checks the scenario
  against them (`scenario.service_problems()`) before anything starts. A run built with `roles=`
  skips the call; the self-test does that, so it can't reach Docker.
- Every `generator` service, one per `[[ledger.source]]`, mounts the config `[configs]` names
  under its service name, through its own `compose.env` variable (`scenario.config_env()`), as
  does every SUT service after `logit` (`Scenario.suts`). The end sequence stops every generator
  before it waits on the sinks, then each SUT service in chain order: an earlier one once its
  `[[ledger.hop]]` sink's queue is empty and nothing is in flight, the last, which holds the
  `[[ledger.sink]]` sinks, once their stored totals hold or their queues are empty.
- Every `backend` service has its own published port, re-read after any revert of a fault on
  it. Freshness is sampled per `[[ledger.sink]]`, the end sequence waits until every sink's stored
  total holds, and each sink's export lands in `vm-export/<sink>.jsonl`.
- An external target (`[target] kind = "external"`) runs without the `local` compose profile,
  so with no backend. It passes compose a second `--env-file`: a plain `KEY=value` copy of the
  variables `SOAK_EXTERNAL_ENV` sets for the target, in a private temporary directory outside
  the repo and the run directory, removed when `execute()` returns. Nothing queries the
  destination: no readiness, freshness, flush, or export. The end sequence waits for every SUT
  sink's queue to empty instead of for the stored totals to hold.
- A Kafka target (`[target] kind = "kafka"`) runs the `kafka` compose profile, a local broker
  with the role `broker`, in place of `local`, with no env-file copy. Like an external target it
  has no backend, so its end sequence waits for the sink's queue to empty; after the SUT stops,
  it reads the target's topic back from the broker into `kafka-topic.ndjson` (topic.py), the
  ledger's V.
- A stand-in target (`[target] kind = "standin"`) runs the `standin` compose profile, the scripted
  destination with the role `standin`, in place of `local`. Its control port is published like a
  backend's and re-read after any revert of a fault on it; a `respond` step POSTs its cue there.
  `compose.env` carries its `[target] shape` for standin.py's `--shape`.
  The end sequence waits for the sink's queue to empty, as for a Kafka target. Its record is its
  stdout, which the log capture collects with every other service's.
- A scenario with a `file` wire or a `rename` step gets `<run>/shared`, mode 0777 so the
  image's unprivileged user, whatever its uid, can write it, named by `SOAK_SHARED_DIR` in
  `compose.env`; its override mounts it into the services that write or read there, and a
  `rename` moves files inside it on the host. For a file wire the end sequence stops the writer,
  counts the lines in every file there (`shared_counted`, which the checks hold the writer's G
  to while it deleted no file), and waits until the tailers' W reaches G or stops growing
  (`tail_caught_up`), reading G and W from log chunks it captures for the purpose, before it
  waits on the sinks.
- A scenario with a `[tls]` table gets `<run>/tls`, named by `SOAK_TLS_DIR` in `compose.env`,
  which its override mounts read-only at `/tls`. `prepare()` fills it before anything starts,
  through the netem image's certs.sh run as the host user: a private CA, the probe's client
  certificate, and each certificate's first generation behind its `live` symlink. A `rotate`
  step issues the next generation, and a `probe` reads what a listener serves (faults.py).
- Under `[overlap]` the instances share their holder's network namespace, so `prepare()` also
  reads each service's `network_mode` from compose and checks each instance is in the holder's.
  Each instance runs the whole pipeline, so the end sequence waits once on every sink's stored
  total, which holds all of them, and then stops each instance.

Every time in `timeline.jsonl`, `watchdog.jsonl`, `stats.ndjson`, and `vm-freshness.jsonl` is
epoch seconds (`t`, `started_at`, `finished_at`), with `offset` measured from the timeline's zero:
the moment the stack was up and, for a local target, VictoriaMetrics answered `/health`.
"""

import json
import os
import shutil
import signal
import sys
import time
import traceback
from pathlib import Path

from . import checks, collect, faults, report, scenario as scenario_mod, telemetry, topic, vm
from .docker import Docker

NETEM_IMAGE = "logit-soak-netem:local"
VM_IMAGE = "victoriametrics/victoria-metrics:v1.152.0"
KAFKA_IMAGE = "apache/kafka:4.2.0"
STANDIN_IMAGE = "python:3.13.16-alpine3.24"

INSPECT_EVERY_S = 5
SLOW_POLL_EVERY_S = 30
# Well inside the rotation window compose.yaml's `logging` options leave at the observed line
# rates; tools/soak/README.md's "Long runs" has the arithmetic.
LOG_CHUNK_EVERY_S = 300
POLL_TIMEOUT_S = 15
# How late the 1 s tick may run before the run is marked `stalled`: past the slowest thing the
# loop does, a `docker stop` at its timeout (`STOP_TIMEOUT_S` + 15 s) or a log chunk's 60 s.
STALL_S = checks.STALL_S
# The exit status of a run that stalled, apart from a FAIL's 1: a rerun once the host is awake
# can pass, while a FAIL needs someone to read it.
STALLED_EXIT = 3


def claim_run_dir(out_dir, now=None):
    """Create and return a new run directory, `<UTC stamp>` or `<UTC stamp>-N` (N from 2).

    `mkdir` is the claim: it fails when the name exists, so two runs that start in the same
    second never share a directory. The suffix keeps every name sorting after the bare stamp."""
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(now))
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    for n in range(1, 10_000):
        path = out_dir / (stamp if n == 1 else f"{stamp}-{n}")
        try:
            path.mkdir()
        except FileExistsError:
            continue
        return path
    raise RuntimeError(f"soak: no free run directory for {stamp} under {out_dir}")
# How long `--retry-stalled` waits before a rerun: past the run directory's 1 s stamp, and long
# enough for the previous project's `down` to settle on the daemon.
RETRY_WAIT_S = 30
READY_BOUND_S = 60
FRESHNESS_LOOKBACK_S = 120
QUIET_BOUND_S = 90
GENERATOR_STOP_T = 30
SUT_STOP_T = 60
# An external or Kafka target's end sequence: how long to wait for the SUT sink's queue to
# empty, over a sink's default `retry_max_delay` (10 s) several times, and how much of the SUT's
# stdout each poll reads, several drains of `internal` lines.
SINK_DRAIN_BOUND_S = 120
SINK_DRAIN_TAIL_LINES = 400
# How long the end sequence waits for a file wire's tailers to read what the writer wrote, how
# often it reads W, and how many reads in a row with W unchanged mean the tailers are done: a
# killed life's last drain interval of counts is never exported, so W can stop short of G.
TAIL_CATCHUP_BOUND_S = 180
TAIL_CATCHUP_EVERY_S = 5
TAIL_STALL_READS = 4
DEFAULT_AGGREGATE_INTERVAL_S = checks.DEFAULT_AGGREGATE_INTERVAL_S
# The compose profile holding the backends, enabled for a local target only, and the one holding
# the `kafka` broker, enabled for a Kafka target only.
LOCAL_PROFILE = "local"
KAFKA_PROFILE = "kafka"
STANDIN_PROFILE = "standin"
# The port a backend serves VictoriaMetrics' API on inside its container.
BACKEND_PORT = 8428
# The port a stand-in takes its cues and answers `/health` on inside its container.
STANDIN_CONTROL_PORT = 8081
STANDIN_UP_BOUND_S = 60
# How long the topic read waits for its consumer: its own idle timeout, then the process.
TOPIC_READ_TIMEOUT_S = 180


class Abort(Exception):
    pass


def _next_multiple(offset, period):
    """The first multiple of `period` after `offset`: a poll's next deadline. A poll that ran
    late skips the deadlines it missed instead of running them back to back."""
    return (int(offset // period) + 1) * period


def _now():
    return time.time()


def _print(text, stream=None):
    stream = stream or sys.stdout
    try:
        print(text, file=stream, flush=True)
    except OSError:
        # Swap in /dev/null so the interpreter's flush at exit can't fail the exit code too.
        devnull = open(os.devnull, "w")
        if stream is sys.stdout:
            sys.stdout = devnull
        else:
            sys.stderr = devnull


def interrupt_on_signals():
    """Makes SIGTERM and SIGHUP raise `KeyboardInterrupt`, except one this process inherited as
    ignored: `nohup` ignores SIGHUP so a run outlives its terminal, and bash resends SIGHUP to
    its jobs when it exits."""
    for signum in (signal.SIGTERM, signal.SIGHUP):
        if signal.getsignal(signum) is not signal.SIG_IGN:
            signal.signal(signum, signal.default_int_handler)


def profiles_for(scenario):
    """The compose profiles a scenario's stack runs: its target's backend, if it has one."""
    if scenario.has_vm:
        return [LOCAL_PROFILE]
    if scenario.kafka:
        return [KAFKA_PROFILE]
    if scenario.standin:
        return [STANDIN_PROFILE]
    return []


def exit_code(results, ended=None):
    """130 for an interrupted run, `STALLED_EXIT` for one whose `run` row reads a stall, 1 for an
    aborted or errored one or any `FAIL` row, else 0. `ended` is the phase the driver ended on:
    `interrupted`, `aborted`, `error`, or None. A stall wins over every status but 130, because
    nothing else in a stalled run can be judged."""
    if ended == "interrupted":
        return 130
    if checks.stalled(results):
        return STALLED_EXIT
    if ended in ("aborted", "error"):
        return 1
    return 1 if any(result.status == checks.FAIL for result in results) else 0


class Run:
    def __init__(self, root, scenario, duration, seed, keep, out_dir, argv, image,
                 external_env=None, roles=None):
        self.root = Path(root)
        self.scenario = scenario
        self.external = scenario.external
        self.has_vm = scenario.has_vm
        self.kafka = scenario.kafka
        self.standin = scenario.standin
        # The operator's env file for an external target. Compose reads `private_env`, a plain
        # copy outside the run directory, since the file can be in shell `export` form and
        # holds credentials.
        self.external_env = external_env
        self.private_env = None
        self.duration = duration
        # The seed a random schedule is drawn with: --seed, else the scenario's own.
        self.seed = scenario.seed_for(seed)
        self.seed_source = None if self.seed is None else ("--seed" if seed is not None
                                                            else "scenario.toml")
        self.keep = keep
        self.argv = argv
        self.image = image
        self.steps = scenario_mod.expand(scenario, duration, self.seed)
        self.out_dir = Path(out_dir)
        # Set by `prepare()`, which creates the directory.
        self.run_dir = None
        self.project = f"soak-{scenario.name}"
        self.t0 = None
        self.ids = {}
        # `{service: role}`: given (the self-test), else resolved from compose in `prepare()`.
        self.roles = None if roles is None else dict(roles)
        # `{service: network_mode}` from compose, resolved with the roles; None when the roles
        # were given.
        self.modes = None
        # Each backend's base URL, by service, and each stand-in's control URL.
        self.vm_base = {}
        self.standin_base = {}
        self.active = {}
        self.down = set()
        self.paused = set()
        # Services inside an `exit` step: running, or exited by the event it names.
        self.exiting = set()
        self.pending_ready = {}
        self.started_at = {}
        self.fail_fast = None
        # `<run>/shared`, made in `prepare()` for a file wire or a `rename` step, else None.
        self.shared_dir = None
        # `<run>/tls`, made in `prepare()` for a scenario with a `[tls]` table, else None.
        self.tls_dir = None

    # ---- setup ---------------------------------------------------------------------------------

    def prepare(self):
        self.run_dir = claim_run_dir(self.out_dir)
        (self.run_dir / "logs").mkdir()
        env_file = self.run_dir / "compose.env"
        # `--env-file` rather than the environment: `sudo` resets the environment.
        env_file.write_text(
            f"SOAK_IMAGE={self.image}\n"
            f"{scenario_mod.SUT_CONFIG_ENV}={self.scenario.config_path('sut')}\n"
            + "".join(f"{scenario_mod.config_env(service)}="
                      f"{self.scenario.config_path(service)}\n"
                      for service in self.scenario.generators + self.scenario.extra_suts)
            + (f"{scenario_mod.STANDIN_SHAPE_ENV}={self.scenario.standin_shape}\n"
               if self.standin else "")
            + (f"{scenario_mod.SHARED_DIR_ENV}={self.make_shared_dir()}\n"
               if self.scenario.uses_shared_dir else "")
            + (f"{scenario_mod.TLS_DIR_ENV}={self.make_tls_dir()}\n"
               if self.scenario.uses_tls_dir else "")
        )
        shutil.copy(self.scenario.path, self.run_dir / "scenario.toml")
        # The checks read the SUT's `receive:` limits from this copy.
        (self.run_dir / "configs").mkdir()
        for key in sorted(self.scenario.configs):
            shutil.copy(self.scenario.config_path(key),
                        self.run_dir / "configs" / self.scenario.configs[key])
        compose_files = [self.root / "compose.yaml"]
        if self.scenario.compose is not None:
            shutil.copy(self.scenario.compose_path(), self.run_dir / "configs" /
                        self.scenario.compose)
            compose_files.append(self.scenario.compose_path())
        if self.external:
            self.private_env = scenario_mod.private_env_copy(self.external_env,
                                                             self.scenario.target["env"])
        env_files = [env_file] + ([self.private_env] if self.external else [])
        profiles = profiles_for(self.scenario)
        self.docker = Docker(self.project, compose_files, env_files, profiles)
        if self.roles is None:
            self.roles = self.resolve_roles()
        scenario_mod.check_services(self.scenario, self.roles, self.modes)
        resolved = self.scenario.to_json(self.duration, self.seed, services=self.roles)
        (self.run_dir / "scenario.resolved.json").write_text(
            json.dumps(resolved, indent=2, sort_keys=True) + "\n"
        )
        self.ctx = faults.Context(docker=self.docker, ids=self.ids, netem_image=NETEM_IMAGE,
                                  standin_base=self.standin_base, shared_dir=self.shared_dir,
                                  tls_dir=self.tls_dir, tls_certs=dict(self.scenario.tls or {}),
                                  user=f"{os.getuid()}:{os.getgid()}")
        self.timeline = collect.Jsonl(self.run_dir / "timeline.jsonl")
        self.watchdog = collect.Jsonl(self.run_dir / "watchdog.jsonl")
        self.stats = collect.Jsonl(self.run_dir / "stats.ndjson")
        self.freshness = collect.Jsonl(self.run_dir / "vm-freshness.jsonl")
        self.capture = collect.LogCapture(self.run_dir, self.docker)
        if self.tls_dir is not None:
            self.issue_tls()

    def make_tls_dir(self):
        """Creates `<run>/tls` for the private CA and the `[tls]` certificates and returns its
        path. certs.sh writes it as the host user; the mode is 0755 whatever the umask, so the
        image's `logit` user, another uid, can traverse the bind mount. The chmod follows the
        mkdir because the umask narrows the mode `mkdir` is given."""
        tls = self.run_dir / scenario_mod.TLS_DIR
        tls.mkdir()
        os.chmod(tls, 0o755)
        self.tls_dir = tls.resolve()
        return self.tls_dir

    def issue_tls(self):
        """Makes the run's CA and the probe's client certificate, then issues each `[tls]`
        certificate's first generation, before anything starts: a config that reads one fails
        its start without it. Records the first issues as the `tls_issued` phase, which
        `tls.served` reads as the serial in force before any rotation. Raises `ScenarioError`
        when certs.sh fails."""
        issued = {}
        calls = [("init", [], None)] + [
            ("issue", faults.issue_args(name, 1, scenario_mod.TLS_INITIAL_DAYS, cert), name)
            for name, cert in sorted(self.ctx.tls_certs.items())]
        for command, args, name in calls:
            result = self.docker.run_oneshot(faults.certs_argv(self.ctx, command, *args))
            report = faults.cert_report(result)
            if not result.ok or report is None:
                shown = " ".join([command, *args])
                raise scenario_mod.ScenarioError([
                    f"certs.sh {shown} failed (rc {result.rc}): "
                    f"{result.stderr.strip()[-500:]}; {NETEM_IMAGE} needs openssl and "
                    "certs.sh, so rebuild it without SOAK_SKIP_IMAGE"])
            if name is not None:
                issued[name] = {"gen": 1, "days": scenario_mod.TLS_INITIAL_DAYS, **report}
        self.ctx.state[faults.TLS_GEN_KEY] = {name: 1 for name in issued}
        self.phase("tls_issued", certs=issued)

    def make_shared_dir(self):
        """Creates `<run>/shared` for a file wire's writer and tailer and returns its path. The
        chmod follows the mkdir because the umask narrows the mode `mkdir` is given."""
        shared = self.run_dir / scenario_mod.SHARED_DIR
        shared.mkdir()
        os.chmod(shared, 0o777)
        self.shared_dir = shared.resolve()
        return self.shared_dir

    def resolve_roles(self):
        """`{service: role}` from the merged compose config, raising `ScenarioError` when compose
        can't produce it."""
        result = self.docker.config_json()
        if not result.ok:
            raise scenario_mod.ScenarioError([
                f"docker compose config failed (rc {result.rc}): {result.stderr.strip()[-500:]}"])
        try:
            self.modes = scenario_mod.network_modes_from_config(result.stdout)
            return scenario_mod.roles_from_config(result.stdout)
        except ValueError as err:
            raise scenario_mod.ScenarioError([f"docker compose config: {err}"]) from None

    def services(self, *roles):
        return sorted(name for name, role in self.roles.items() if role in roles)

    def say(self, text):
        offset = "" if self.t0 is None else f"[{scenario_mod.format_duration(_now() - self.t0)}] "
        _print(f"soak: {offset}{text}")

    def offset(self, t):
        return None if self.t0 is None else round(t - self.t0, 3)

    def phase(self, name, **extra):
        t = _now()
        record = {"event": "phase", "phase": name, "t": t, "offset": self.offset(t)}
        record.update(extra)
        self.timeline.write(record)

    # ---- the run -------------------------------------------------------------------------------

    def execute(self):
        """Runs everything after `prepare()`, then removes the private env copy. Returns the
        process exit code."""
        try:
            return self._execute()
        finally:
            scenario_mod.remove_private_env(self.private_env)

    def _execute(self):
        if self.docker.project_containers():
            _print(f"soak: compose project '{self.project}' already has containers on this "
                   "daemon.\n  Not this run's to stop. If it is yours, run:\n"
                   f"  {' '.join(self.docker.prefix)} compose -p {self.project} "
                   + "".join(f"-f {path} " for path in self.docker.compose_files) + "down -v")
            return 2
        images = ([self.image, NETEM_IMAGE] + ([VM_IMAGE] if self.has_vm else [])
                  + ([KAFKA_IMAGE] if self.kafka else [])
                  + ([STANDIN_IMAGE] if self.standin else []))
        collect.provenance(self.run_dir, self.root.parent.parent, self.docker,
                           images, self.scenario.name,
                           self.duration, self.seed, self.argv, self.seed_source)
        self.say(f"run directory {self.run_dir}")
        ended = None
        try:
            self.up()
            self.loop()
            self.end_sequence()
        except KeyboardInterrupt:
            ended = "interrupted"
            self.phase("interrupted")
            self.say("interrupted; collecting and tearing down")
        except Abort as err:
            ended = "aborted"
            self.phase("aborted", reason=str(err))
            self.say(f"aborted: {err}")
        except Exception:
            ended = "error"
            self.phase("error", traceback=traceback.format_exc())
            self.say("driver error; collecting and tearing down")
            _print(traceback.format_exc(), sys.stderr)
        finally:
            self.teardown()
        results = checks.run_all(self.run_dir)
        report.write(self.run_dir, results)
        _print(report.markdown_table(results))
        self.say(f"results in {self.run_dir}/results.md")
        return exit_code(results, ended)

    def up(self):
        self.say(f"starting compose project {self.project}")
        self.phase("up_begin")
        result = self.docker.compose("up", "-d", "--wait", "--wait-timeout", "120", timeout=240)
        self.refresh_ids()
        self.phase("up_end", rc=result.rc, stderr=result.stderr.strip()[-2000:])
        if not result.ok:
            raise Abort(f"compose up failed: {result.stderr.strip()[-500:]}")
        for backend in self.services(scenario_mod.BACKEND_ROLE):
            self.refresh_vm_base(backend)
            deadline = _now() + 60
            while not (self.vm_base.get(backend) and vm.healthy(self.vm_base[backend])):
                if _now() > deadline:
                    raise Abort(f"{backend} never answered /health")
                time.sleep(1)
                self.refresh_vm_base(backend)
        for standin in self.services(scenario_mod.STANDIN_ROLE):
            self.refresh_standin_base(standin)
            deadline = _now() + STANDIN_UP_BOUND_S
            while not (self.standin_base.get(standin)
                       and vm.healthy(self.standin_base[standin])):
                if _now() > deadline:
                    raise Abort(f"{standin} never answered /health")
                time.sleep(1)
                self.refresh_standin_base(standin)
        for service in self.services(*scenario_mod.LOGIT_ROLES):
            info = self.docker.inspect(self.ids[service]) or {}
            self.started_at[service] = (info.get("State") or {}).get("StartedAt")
        self.t0 = _now()
        # `marks_stalls` tells the checks this driver records a late tick as a `stalled` phase.
        self.phase("start", t0=self.t0, ids=dict(self.ids), marks_stalls=True)
        seed = "" if self.seed is None else f", seed {self.seed}"
        self.say(f"stack up; {len(self.steps)} fault(s) over "
                 f"{scenario_mod.format_duration(self.duration)}{seed}")

    def refresh_ids(self):
        for service in sorted(self.roles):
            container = self.docker.container_id(service)
            if container:
                self.ids[service] = container

    def refresh_vm_base(self, backend):
        address = self.docker.port(backend, BACKEND_PORT)
        self.vm_base[backend] = f"http://{address}" if address else None

    def refresh_standin_base(self, standin):
        address = self.docker.port(standin, STANDIN_CONTROL_PORT)
        self.standin_base[standin] = f"http://{address}" if address else None

    def loop(self):
        queue = []
        for step in self.steps:
            queue.append((step.start, 1, "apply", step))
            queue.append((step.end, 0, "revert", step))
        # A revert sorts before an apply due at the same instant.
        queue.sort(key=lambda item: (item[0], item[1]))
        next_inspect = 0.0
        next_slow = 0.0
        next_chunk = LOG_CHUNK_EVERY_S
        tick = 0
        while True:
            now = _now()
            offset = now - self.t0
            if offset - tick > STALL_S:
                # Nothing in the loop takes this long: the host slept or the driver was
                # stopped, and neither the schedule nor the polls ran meanwhile. Every step
                # overdue now would run back to back, so none runs; the end sequence reverts
                # the open ones.
                self.phase("stalled", late_s=round(offset - tick, 3), since=tick)
                self.phase("aborted", reason=checks.STALL_ABORT_REASON, since=tick)
                self.say(f"the tick ran {offset - tick:.0f}s late; the run can't be judged, so "
                         "the schedule stops here")
                return
            if offset >= self.duration:
                break
            while queue and queue[0][0] <= offset:
                _, _, kind, step = queue.pop(0)
                self.act(kind, step)
            self.poll_ready()
            if offset >= next_inspect:
                next_inspect = _next_multiple(offset, INSPECT_EVERY_S)
                self.inspect_all()
                if self.fail_fast:
                    self.phase("fail_fast", reason=self.fail_fast)
                    self.say(f"fail fast: {self.fail_fast}")
                    return
            if offset >= next_slow:
                next_slow = _next_multiple(offset, SLOW_POLL_EVERY_S)
                self.sample_stats()
                if self.has_vm:
                    self.sample_freshness()
            if offset >= next_chunk:
                next_chunk = _next_multiple(offset, LOG_CHUNK_EVERY_S)
                self.capture.chunk(self.ids)
            tick += 1
            delay = self.t0 + tick - _now()
            if delay > 0:
                time.sleep(delay)
            else:
                tick = int(_now() - self.t0)

    # ---- actions -------------------------------------------------------------------------------

    def act(self, kind, step, early=False):
        action = faults.ACTIONS[step.action]
        started = _now()
        record = {
            "event": kind, "step": step.id, "action": step.action, "on": step.on,
            "args": step.args, "planned_offset": step.end if kind == "revert" else step.start,
            "started_at": started, "offset": self.offset(started),
            **{key: action.affects(wire, step, self.roles)
               for wire, key in faults.INGRESS_KEYS.items()},
            "affects_egress": bool(action.affects_egress(step, self.roles)),
            **step.expected(),
        }
        if early:
            record["early"] = True
        result = (action.apply if kind == "apply" else action.revert)(self.ctx, step)
        record.update(result)
        record["finished_at"] = _now()
        self.timeline.write(record)
        self.after(kind, step, result)
        show = f" -> {result['netem_show']}" if result.get("netem_show") else ""
        self.say(f"{kind} {step.id} {step.action} on {step.on} rc={result['rc']}{show}")
        if kind == "apply":
            self.active[step.id] = step
        else:
            self.active.pop(step.id, None)

    def after(self, kind, step, result):
        """Keeps the expected-state sets in step with an action, and queues readiness probes."""
        service = step.on
        restarted = False
        if step.action == "exit":
            if kind == "apply":
                self.exiting.add(service)
            else:
                self.exiting.discard(service)
                restarted = bool(result.get("started"))
        elif step.action in faults.STARTS_ON_REVERT:
            if kind == "apply":
                self.down.add(service)
            else:
                self.down.discard(service)
                restarted = True
        elif step.action == "pause":
            if kind == "apply":
                self.paused.add(service)
            else:
                self.paused.discard(service)
                if self.roles.get(service) in scenario_mod.LOGIT_ROLES:
                    self.pending_ready[service] = {"since": _now(), "step": step.id,
                                                   "why": "unpause"}
        elif step.action in faults.STARTS_ON_APPLY and kind == "apply":
            restarted = True
        if restarted:
            info = self.docker.inspect(self.ids[service]) or {}
            self.started_at[service] = (info.get("State") or {}).get("StartedAt")
            if self.roles.get(service) in scenario_mod.LOGIT_ROLES:
                self.pending_ready[service] = {"since": _now(), "step": step.id, "why": "start"}
        # Docker can publish a new host port on a start and on a network reconnect, so any
        # revert re-reads it; the freshness poll would otherwise query a dead port.
        if restarted or kind == "revert":
            if self.roles.get(service) == scenario_mod.BACKEND_ROLE:
                self.refresh_vm_base(service)
            elif self.roles.get(service) == scenario_mod.STANDIN_ROLE:
                self.refresh_standin_base(service)

    def poll_ready(self):
        for service, pending in list(self.pending_ready.items()):
            if service in self.paused or service in self.down:
                continue
            # With no `--admin`, `logit ready` probes the container's own `LOGIT_ADMIN`, else
            # http://127.0.0.1:9600, as the image's HEALTHCHECK does: an overlap instance serves
            # its admin endpoint on a Unix socket, since the instances share one address.
            result = self.docker.exec_(self.ids[service], ["logit", "ready"], timeout=15)
            now = _now()
            waited = now - pending["since"]
            if result.ok or waited > READY_BOUND_S:
                self.timeline.write({
                    "event": "ready", "on": service, "step": pending["step"],
                    "why": pending["why"], "ok": result.ok, "ready_s": round(waited, 3),
                    "t": now, "offset": self.offset(now),
                    "output": (result.stdout + result.stderr).strip()[-500:],
                })
                self.say(f"{service} {'ready' if result.ok else 'NOT ready'} after {waited:.0f}s")
                del self.pending_ready[service]

    # ---- watchdog polls ------------------------------------------------------------------------

    def inspect_all(self):
        now = _now()
        for service, container in self.ids.items():
            info = self.docker.inspect(container, timeout=POLL_TIMEOUT_S)
            if info is None:
                self.watchdog.write({"t": now, "offset": self.offset(now), "svc": service,
                                     "error": "inspect failed"})
                if _now() - now >= POLL_TIMEOUT_S:
                    # A timeout means the daemon isn't answering; the next round retries.
                    return
                continue
            state = info.get("State") or {}
            health = state.get("Health") or {}
            log = health.get("Log") or []
            record = {
                "t": now, "offset": self.offset(now), "svc": service, "id": container,
                "status": state.get("Status"), "paused": state.get("Paused"),
                "exit_code": state.get("ExitCode"), "oom_killed": state.get("OOMKilled"),
                "started_at": state.get("StartedAt"), "finished_at": state.get("FinishedAt"),
                "restart_count": info.get("RestartCount"),
                "health": health.get("Status"),
                "health_log": log[-1] if log else None,
            }
            self.watchdog.write(record)
            if self.roles.get(service) in scenario_mod.LOGIT_ROLES and not self.fail_fast:
                self.check_expected(service, record)

    def check_expected(self, service, record):
        status = record["status"]
        if service in self.down:
            expected = ("exited",)
        elif service in self.exiting:
            expected = ("running", "exited")
        elif service in self.paused:
            expected = ("paused",)
        else:
            expected = ("running",)
        if status not in expected:
            self.fail_fast = (f"{service} is {status} (exit code {record['exit_code']}) with no "
                              "step behind it")
        elif record["started_at"] != self.started_at.get(service):
            self.fail_fast = f"{service} restarted with no step behind it"

    def sample_stats(self):
        running = [c for s, c in self.ids.items() if s not in self.down and s not in self.paused]
        now = _now()
        for sample in self.docker.stats(running, timeout=POLL_TIMEOUT_S):
            service = next((s for s, c in self.ids.items() if c.startswith(sample.get("ID", "-"))
                            or sample.get("ID", "-").startswith(c)), sample.get("Name"))
            self.stats.write({"t": now, "offset": self.offset(now), "svc": service, **sample})

    def sample_freshness(self):
        """One record per sink: the age of the newest sample its selector matches."""
        for sink in self.scenario.sinks:
            now = _now()
            backend = sink["backend"]
            record = {"t": now, "offset": self.offset(now), "sink": sink["id"],
                      "backend": backend}
            if backend in self.down or backend in self.paused:
                record["skipped"] = f"{backend} held by a step"
            elif not self.vm_base.get(backend):
                record["error"] = f"{backend} has no published port"
            else:
                try:
                    record.update(vm.freshness(self.vm_base[backend], sink["vm_selector"], now,
                                               FRESHNESS_LOOKBACK_S))
                except vm.VmError as err:
                    record["error"] = str(err)
            self.freshness.write(record)

    # ---- end -----------------------------------------------------------------------------------

    def end_sequence(self):
        self.phase("end_begin")
        self.say("end sequence")
        for step in sorted(self.active.values(), key=lambda s: s.start, reverse=True):
            self.act("revert", step, early=True)
        self.inspect_all()

        for service in self.services(scenario_mod.GENERATOR_ROLE):
            result = self.docker.run(["stop", "-t", str(GENERATOR_STOP_T), self.ids[service]],
                                     timeout=GENERATOR_STOP_T + 15)
            self.phase("generator_stopped", service=service, rc=result.rc,
                       stderr=result.stderr.strip())

        self.wait_tailed()
        if self.scenario.overlap is not None:
            self.stop_instances(self.scenario.suts)
        else:
            self.stop_chain(self.scenario.suts)
        self.finish_end_sequence()

    def stop_chain(self, suts):
        """Stops each SUT service in chain order: an earlier one once its `[[ledger.hop]]` sink
        is drained, the last once its sinks' stored totals hold or their queues are empty."""
        for service in suts:
            if service != suts[-1]:
                self.wait_sink_drained(service, sinks=[hop["sink"] for hop in self.scenario.hops
                                                       if hop.get("from") == service])
            elif self.has_vm:
                self.wait_quiet()
            else:
                self.wait_sink_drained(service)
            self.stop_sut(service)

    def stop_sut(self, service):
        result = self.docker.run(["stop", "-t", str(SUT_STOP_T), self.ids[service]],
                                 timeout=SUT_STOP_T + 15)
        info = self.docker.inspect(self.ids[service]) or {}
        self.phase("sut_stopped", service=service, rc=result.rc,
                   stderr=result.stderr.strip(),
                   exit_code=(info.get("State") or {}).get("ExitCode"))

    def stop_instances(self, instances):
        """A rolling overlap's end: every instance runs the whole pipeline and every sink's
        backend holds all of them, so one wait covers the instances together, the stored totals
        holding (or, with no backend, each instance's sinks drained), and then each instance
        stops, with nothing left arriving to move between them."""
        if self.has_vm:
            self.wait_quiet()
        else:
            for service in instances:
                self.wait_sink_drained(service)
        for service in instances:
            self.stop_sut(service)

    def finish_end_sequence(self):
        if self.kafka:
            self.read_topic()
        if not self.has_vm:
            self.phase("end_end")
            return
        export_dir = self.run_dir / "vm-export"
        export_dir.mkdir()
        for backend in self.services(scenario_mod.BACKEND_ROLE):
            self.refresh_vm_base(backend)
        for sink in self.scenario.sinks:
            export_path = export_dir / f"{sink['id']}.jsonl"
            base = self.vm_base.get(sink["backend"])
            try:
                vm.force_flush(base)
                text = vm.export_raw(base, sink["vm_selector"], 0, timeout=120)
                export_path.write_text(text)
                self.phase("vm_exported", sink=sink["id"], series=len(vm.parse_export(text)))
            except (vm.VmError, TypeError) as err:
                export_path.write_text("")
                self.phase("vm_export_failed", sink=sink["id"], error=str(err))
        self.phase("end_end")

    def read_topic(self):
        """Reads the Kafka target's topic back from the broker after the SUT stopped: each
        partition's end offset into `kafka-offsets.txt`, then that many records from the
        beginning into `kafka-topic.ndjson`, one record value per line, the ledger's V. Records
        `kafka_read` with both counts, or `kafka_read_failed` when either read fails or the two
        disagree."""
        name = self.scenario.target["topic"]
        container = self.ids.get("kafka")
        topic_path = self.run_dir / topic.TOPIC_FILE
        if not container:
            topic_path.write_text("")
            self.phase("kafka_read_failed", error="no kafka container")
            return
        result = self.docker.exec_(container, topic.offsets_argv(name), timeout=POLL_TIMEOUT_S * 4)
        (self.run_dir / topic.OFFSETS_FILE).write_text(result.stdout)
        offsets = topic.end_offsets(result.stdout)
        if not result.ok or offsets is None:
            topic_path.write_text("")
            self.phase("kafka_read_failed", rc=result.rc, step="offsets",
                       stderr=result.stderr.strip()[-2000:])
            return
        expected = sum(offsets.values())
        result = self.docker.exec_(container, topic.consumer_argv(name, expected),
                                   timeout=TOPIC_READ_TIMEOUT_S)
        topic_path.write_text(result.stdout)
        records = len(topic.record_lines(result.stdout))
        processed = topic.processed_count(result.stderr)
        extra = dict(rc=result.rc, topic=name, offsets=offsets, end_offsets=expected,
                     records=records, processed=processed)
        if records != expected or (processed is not None and processed != records):
            self.phase("kafka_read_failed", step="consume", **extra,
                       stderr=result.stderr.strip()[-2000:])
            return
        self.phase("kafka_read", **extra)

    def wait_quiet(self):
        """Waits until every sink's stored total holds for two aggregate windows, bounded at
        `QUIET_BOUND_S`. The window is the stored samples' spacing, the widest across sinks:
        `aggregate` writes each series once per window while it has data, and stops writing an
        idle one, so newer timestamps can't mark the windows once the generators have stopped."""
        for backend in self.services(scenario_mod.BACKEND_ROLE):
            self.refresh_vm_base(backend)
        deadline = _now() + QUIET_BOUND_S
        last_totals = None
        since = _now()
        hold_s = None
        polls = 0
        while _now() < deadline:
            polls += 1
            totals = {}
            spacing = []
            try:
                for sink in self.scenario.sinks:
                    base = self.vm_base.get(sink["backend"])
                    vm.force_flush(base)
                    series = vm.export(base, sink["vm_selector"], int(_now() - 300))
                    totals[sink["id"]] = vm.last_total(series)
                    spacing.append(vm.sample_spacing_s(series))
            except (vm.VmError, TypeError):
                time.sleep(2)
                continue
            if hold_s is None and all(spacing):
                hold_s = 2 * max(spacing)
            if totals != last_totals:
                last_totals = totals
                since = _now()
            elif hold_s is not None and _now() - since >= hold_s:
                self.phase("quiet", held=True, totals=totals, hold_s=hold_s, polls=polls)
                return
            time.sleep(2)
        self.phase("quiet", held=False, totals=last_totals, hold_s=hold_s, polls=polls,
                   bound_s=QUIET_BOUND_S)

    def wait_tailed(self):
        """For a file wire: counts the lines the writer left in `<run>/shared`
        (`collect.count_lines`, phase `shared_counted`), reads G, the writers' `file_out`
        `logit.component.events.delivered` summed over every life, then waits, bounded at
        `TAIL_CATCHUP_BOUND_S`, until the tailing listeners' W summed over every life reaches G
        or holds for `TAIL_STALL_READS` reads (phase `tail_caught_up`). It waits for G, not the
        count, because a writer that deleted a file left fewer lines than it wrote, and W
        reaches that count before the tailers read to the end of the files.

        The writers have stopped, but a log chunk ends `LOG_LAG_NS` before it's taken, so G is
        read from a chunk of each writer taken that long after the wait began. Each read of W
        captures a log chunk of the ingress SUT, so W is read from the whole of its stdout."""
        sources = self.scenario.file_sources
        if not sources:
            return
        counted = collect.count_lines(self.run_dir / scenario_mod.SHARED_DIR)
        self.phase("shared_counted", **counted)
        time.sleep(collect.LOG_LAG_NS / 1e9 + 1)
        g = 0
        for writer in sorted({source["service"] for source in sources}):
            self.capture.chunk({writer: self.ids[writer]})
            tel = telemetry.read_ndjson(self.run_dir / "logs" / f"{writer}.stdout")
            g += sum(sum(tel.counter_by_life("logit.component.events.delivered",
                                             component=sink).values())
                     for source in sources if source["service"] == writer
                     for sink in scenario_mod.source_ids(source, "generator_sink"))
        listeners = [source["sut_listener"] for source in sources]
        service = scenario_mod.SUT_SERVICE
        deadline = _now() + TAIL_CATCHUP_BOUND_S
        last, unchanged, reads, w = None, 0, 0, None
        caught = dict(g=g, counted_lines=counted["lines"])
        while _now() < deadline:
            reads += 1
            self.capture.chunk({service: self.ids[service]})
            tel = telemetry.read_ndjson(self.run_dir / "logs" / f"{service}.stdout")
            w = sum(sum(tel.counter_by_life("logit.input.lines", component=listener).values())
                    for listener in listeners)
            if w >= g:
                self.phase("tail_caught_up", reached=True, w=w, reads=reads, **caught)
                return
            unchanged = unchanged + 1 if w == last else 0
            last = w
            if unchanged >= TAIL_STALL_READS - 1:
                self.phase("tail_caught_up", reached=False, held=True, w=w, reads=reads,
                           **caught)
                return
            time.sleep(TAIL_CATCHUP_EVERY_S)
        self.phase("tail_caught_up", reached=False, held=False, w=w, reads=reads,
                   bound_s=TAIL_CATCHUP_BOUND_S, **caught)

    def aggregate_interval(self):
        """The `aggregate` window of the SUT service holding `sut_aggregate`, in seconds, or
        `DEFAULT_AGGREGATE_INTERVAL_S`; under `split = "target"` the longest of the sinks' own
        `aggregate` windows."""
        try:
            text = self.scenario.sut_config_path(self.scenario.sink_sut).read_text()
        except OSError:
            text = ""
        names = ([sink.get("aggregate") or "" for sink in self.scenario.sinks]
                 if self.scenario.split == scenario_mod.TARGET_SPLIT
                 else [self.scenario.ledger.get("sut_aggregate", "")])
        intervals = [checks.component_duration(text, name, "interval") for name in names]
        return max([interval for interval in intervals if interval] or
                   [DEFAULT_AGGREGATE_INTERVAL_S])

    def wait_sink_drained(self, service=scenario_mod.SUT_SERVICE, sinks=None):
        """Waits until a drain of SUT `service` at least two aggregate windows after the wait
        began shows each sink's `buffer.batches` at 0, and its `retrying` and
        `logit.output.in_flight` (a `logit_out`'s frames not yet acknowledged) at 0 or never
        set, bounded at `SINK_DRAIN_BOUND_S`. The sinks are `sinks`, by default the
        `[[ledger.sink]]` ids; an earlier SUT in the chain is waited on with its
        `[[ledger.hop]]` sink. For the last SUT this is the wait for a target with
        no backend to query (external, Kafka, or a stand-in). Two windows let `aggregate` flush
        the last lines the generators sent; a sink's queue then empties once its last send is
        accepted. Reads the newest `SINK_DRAIN_TAIL_LINES` of the
        service's stdout, which a run in progress hasn't collected yet. The phase names the
        service and records each sink's state under `sinks`."""
        if sinks is None:
            sinks = [sink["id"] for sink in self.scenario.sinks]
        stopped = _now()
        hold_s = 2 * self.aggregate_interval()
        deadline = stopped + SINK_DRAIN_BOUND_S
        polls = 0
        state = {}
        while _now() < deadline:
            polls += 1
            result = self.docker.logs_tail(self.ids[service], SINK_DRAIN_TAIL_LINES,
                                           timeout=POLL_TIMEOUT_S)
            tel = telemetry.parse_ndjson(result.stdout.splitlines())
            newest = max(tel.drains, default=None)
            state = {}
            for sink in sinks:
                queued = tel.gauge_series("logit.component.buffer.batches", component=sink)
                retrying = tel.gauge_series("logit.component.retrying", component=sink)
                in_flight = tel.gauge_series("logit.output.in_flight", component=sink)
                state[sink] = {"buffer_batches": queued[-1][1] if queued else None,
                               "retrying": retrying[-1][1] if retrying else None,
                               "in_flight": in_flight[-1][1] if in_flight else None}
            if (newest is not None and newest >= stopped + hold_s
                    and all(one["buffer_batches"] == 0 and not one["retrying"]
                            and not one["in_flight"] for one in state.values())):
                self.phase("sink_drained", service=service, held=True, hold_s=hold_s,
                           polls=polls, waited_s=round(_now() - stopped, 3),
                           newest_drain=newest, sinks=state)
                return
            time.sleep(2)
        self.phase("sink_drained", service=service, held=False, hold_s=hold_s, polls=polls,
                   bound_s=SINK_DRAIN_BOUND_S, sinks=state)
        self.say(f"a {service} sink's queue didn't empty within {SINK_DRAIN_BOUND_S}s: {state}")

    def teardown(self):
        try:
            self.refresh_ids()
            if self.ids:
                collect.service_logs(self.run_dir, self.docker, self.ids, self.capture)
                collect.service_inspect(self.run_dir, self.docker, self.ids)
            self.phase("collected")
        except KeyboardInterrupt:
            self.say("interrupted during collection")
        finally:
            if self.keep:
                self.say(f"--keep: leaving compose project {self.project} up")
            else:
                result = self.docker.compose("down", "-v", "--remove-orphans", timeout=180)
                self.phase("down", rc=result.rc)
            for jsonl in (self.timeline, self.watchdog, self.stats, self.freshness,
                          self.capture):
                jsonl.close()


def run_retrying(retries, root, scenario_path, duration, seed, keep, out_dir, argv, image,
                 external_env=None):
    """`run()`, rerun up to `retries` times while it returns `STALLED_EXIT`, `RETRY_WAIT_S`
    apart, each in a new run directory with the same duration and seed. Returns the last run's
    exit code, or 130 when interrupted during a wait."""
    for attempt in range(retries + 1):
        code = run(root, scenario_path, duration, seed, keep, out_dir, argv, image, external_env)
        if code != STALLED_EXIT or attempt == retries:
            return code
        _print(f"soak: the run stalled; rerunning in {RETRY_WAIT_S}s "
               f"(retry {attempt + 1} of {retries})")
        try:
            time.sleep(RETRY_WAIT_S)
        except KeyboardInterrupt:
            return 130
    return code


def run(root, scenario_path, duration, seed, keep, out_dir, argv, image, external_env=None):
    scenario = scenario_mod.load(scenario_path)
    duration = scenario.duration if duration is None else duration
    scenario_mod.validate(scenario, duration=duration, seed=seed)
    if scenario.external:
        # script/soak checks this before it validates the configs; a direct run checks it here.
        missing = scenario_mod.missing_env(scenario, external_env)
        if missing:
            raise scenario_mod.ScenarioError([
                f"external target {scenario.target['name']}: {external_env} doesn't set "
                f"{', '.join(missing)} (set SOAK_EXTERNAL_ENV to another file)"])
        unsafe = scenario_mod.unsafe_env(scenario, external_env)
        if unsafe:
            raise scenario_mod.ScenarioError([
                f"external target {scenario.target['name']}: in {external_env}, "
                f"{'; '.join(unsafe)}; {scenario_mod.UNSAFE_ENV_REASON}"])
    run = Run(root, scenario, duration, seed, keep, out_dir, argv, image, external_env)
    # Before `prepare()` writes the private env copy, so a SIGTERM or SIGHUP from here on
    # unwinds through the code that removes it.
    interrupt_on_signals()
    try:
        run.prepare()
    except BaseException:
        scenario_mod.remove_private_env(run.private_env)
        raise
    return run.execute()
