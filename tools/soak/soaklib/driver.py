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
  schedule at once (fail fast). The end sequence, collection, and scoring still run.
- `docker exec` into a paused container blocks, so readiness is never probed while a step holds
  the container paused.
- The `try`/`finally` around the run collects logs and tears the project down on every exit
  but SIGKILL, unless `--keep`. SIGTERM and SIGHUP raise `KeyboardInterrupt` as SIGINT does;
  Python's default for both ends the process without running `finally`. A signal inherited as
  ignored stays ignored, so `nohup` keeps a run alive past a closed terminal.
- After a SIGHUP the terminal can be gone, so output goes through `_print`, which drops it
  rather than fail the run: the run directory holds everything.
- An external target (`[target] kind = "external"`) runs without the `local` compose profile,
  so with no `victoria-metrics`. It passes compose a second `--env-file`: a plain `KEY=value`
  copy of the variables `SOAK_EXTERNAL_ENV` sets for the target, in a private temporary
  directory outside the repo and the run directory, removed when `execute()` returns. Nothing
  queries the destination: no readiness, freshness, flush, or export. The end sequence waits for
  the SUT sink's queue to empty instead of for the stored total to hold.

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

from . import checks, collect, faults, report, scenario as scenario_mod, telemetry, vm
from .docker import Docker

NETEM_IMAGE = "logit-soak-netem:local"
VM_IMAGE = "victoriametrics/victoria-metrics:v1.152.0"
ADMIN = "http://127.0.0.1:9600"

INSPECT_EVERY_S = 5
SLOW_POLL_EVERY_S = 30
# Well inside the rotation window compose.yaml's `logging` options leave at the observed line
# rates; tools/soak/README.md's "Long runs" has the arithmetic.
LOG_CHUNK_EVERY_S = 300
POLL_TIMEOUT_S = 15
READY_BOUND_S = 60
FRESHNESS_LOOKBACK_S = 120
QUIET_BOUND_S = 90
GENERATOR_STOP_T = 30
SUT_STOP_T = 60
# An external target's end sequence: how long to wait for the SUT sink's queue to empty, over a
# sink's default `retry_max_delay` (10 s) several times, and how much of the SUT's stdout each
# poll reads, several drains of `internal` lines.
SINK_DRAIN_BOUND_S = 120
SINK_DRAIN_TAIL_LINES = 400
# The aggregate window assumed when the SUT config names none.
DEFAULT_AGGREGATE_INTERVAL_S = 10.0
# The compose profile holding `victoria-metrics`, enabled for a local target only.
LOCAL_PROFILE = "local"


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


def exit_code(results, ended=None):
    """130 for an interrupted run, 1 for an aborted or errored one or any `FAIL` row, else 0.
    `ended` is the phase the driver ended on: `interrupted`, `aborted`, `error`, or None."""
    if ended == "interrupted":
        return 130
    if ended in ("aborted", "error"):
        return 1
    return 1 if any(result.status == checks.FAIL for result in results) else 0


class Run:
    def __init__(self, root, scenario, duration, seed, keep, out_dir, argv, image,
                 external_env=None):
        self.root = Path(root)
        self.scenario = scenario
        self.external = scenario.external
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
        stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        self.run_dir = Path(out_dir) / stamp
        self.project = f"soak-{scenario.name}"
        self.t0 = None
        self.ids = {}
        self.vm_base = None
        self.active = {}
        self.down = set()
        self.paused = set()
        self.pending_ready = {}
        self.started_at = {}
        self.fail_fast = None

    # ---- setup ---------------------------------------------------------------------------------

    def prepare(self):
        self.run_dir.mkdir(parents=True)
        (self.run_dir / "logs").mkdir()
        env_file = self.run_dir / "compose.env"
        # `--env-file` rather than the environment: `sudo` resets the environment.
        env_file.write_text(
            f"SOAK_IMAGE={self.image}\n"
            f"SOAK_SUT_CONFIG={self.scenario.config_path('sut')}\n"
            f"SOAK_GENERATOR_CONFIG={self.scenario.config_path('generator')}\n"
        )
        shutil.copy(self.scenario.path, self.run_dir / "scenario.toml")
        # The checks read the SUT's `receive:` limits from this copy.
        (self.run_dir / "configs").mkdir()
        for role in sorted(self.scenario.configs):
            shutil.copy(self.scenario.config_path(role),
                        self.run_dir / "configs" / self.scenario.configs[role])
        resolved = self.scenario.to_json(self.duration, self.seed)
        (self.run_dir / "scenario.resolved.json").write_text(
            json.dumps(resolved, indent=2, sort_keys=True) + "\n"
        )
        if self.external:
            self.private_env = scenario_mod.private_env_copy(self.external_env,
                                                             self.scenario.target["env"])
        env_files = [env_file] + ([self.private_env] if self.external else [])
        profiles = [] if self.external else [LOCAL_PROFILE]
        self.docker = Docker(self.project, self.root / "compose.yaml", env_files, profiles)
        self.ctx = faults.Context(docker=self.docker, ids=self.ids, netem_image=NETEM_IMAGE)
        self.timeline = collect.Jsonl(self.run_dir / "timeline.jsonl")
        self.watchdog = collect.Jsonl(self.run_dir / "watchdog.jsonl")
        self.stats = collect.Jsonl(self.run_dir / "stats.ndjson")
        self.freshness = collect.Jsonl(self.run_dir / "vm-freshness.jsonl")
        self.capture = collect.LogCapture(self.run_dir, self.docker)

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
                   f"-f {self.docker.compose_file} down -v")
            return 1
        images = [self.image, NETEM_IMAGE] + ([] if self.external else [VM_IMAGE])
        collect.provenance(self.run_dir, self.root.parent.parent, self.docker,
                           images, self.scenario.name,
                           self.duration, self.seed, self.argv, self.seed_source)
        self.say(f"run directory {self.run_dir}")
        ended = None
        interrupt_on_signals()
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
        if not self.external:
            self.refresh_vm_base()
            deadline = _now() + 60
            while not (self.vm_base and vm.healthy(self.vm_base)):
                if _now() > deadline:
                    raise Abort("VictoriaMetrics never answered /health")
                time.sleep(1)
                self.refresh_vm_base()
        for service in scenario_mod.LOGIT_SERVICES:
            info = self.docker.inspect(self.ids[service]) or {}
            self.started_at[service] = (info.get("State") or {}).get("StartedAt")
        self.t0 = _now()
        self.phase("start", t0=self.t0, ids=dict(self.ids))
        seed = "" if self.seed is None else f", seed {self.seed}"
        self.say(f"stack up; {len(self.steps)} fault(s) over "
                 f"{scenario_mod.format_duration(self.duration)}{seed}")

    def refresh_ids(self):
        for service in scenario_mod.SERVICES:
            container = self.docker.container_id(service)
            if container:
                self.ids[service] = container

    def refresh_vm_base(self):
        address = self.docker.port("victoria-metrics", 8428)
        self.vm_base = f"http://{address}" if address else None

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
                if not self.external:
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
            "affects_udp_ingress": bool(action.affects_udp_ingress(step)),
            "affects_egress": bool(action.affects_egress(step)),
        }
        if early:
            record["early"] = True
        result = (action.apply if kind == "apply" else action.revert)(self.ctx, step)
        record.update(result)
        record["finished_at"] = _now()
        self.timeline.write(record)
        self.after(kind, step)
        show = f" -> {result['netem_show']}" if result.get("netem_show") else ""
        self.say(f"{kind} {step.id} {step.action} on {step.on} rc={result['rc']}{show}")
        if kind == "apply":
            self.active[step.id] = step
        else:
            self.active.pop(step.id, None)

    def after(self, kind, step):
        """Keeps the expected-state sets in step with an action, and queues readiness probes."""
        service = step.on
        restarted = False
        if step.action in faults.STARTS_ON_REVERT:
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
                if service in scenario_mod.LOGIT_SERVICES:
                    self.pending_ready[service] = {"since": _now(), "step": step.id,
                                                   "why": "unpause"}
        elif step.action in faults.STARTS_ON_APPLY and kind == "apply":
            restarted = True
        if restarted:
            info = self.docker.inspect(self.ids[service]) or {}
            self.started_at[service] = (info.get("State") or {}).get("StartedAt")
            if service in scenario_mod.LOGIT_SERVICES:
                self.pending_ready[service] = {"since": _now(), "step": step.id, "why": "start"}
        # Docker can publish a new host port on a start and on a network reconnect, so any
        # revert re-reads it; the freshness poll would otherwise query a dead port.
        if service == "victoria-metrics" and (restarted or kind == "revert"):
            self.refresh_vm_base()

    def poll_ready(self):
        for service, pending in list(self.pending_ready.items()):
            if service in self.paused or service in self.down:
                continue
            result = self.docker.exec_(self.ids[service], ["logit", "ready", "--admin", ADMIN],
                                       timeout=15)
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
            if service in scenario_mod.LOGIT_SERVICES and not self.fail_fast:
                self.check_expected(service, record)

    def check_expected(self, service, record):
        status = record["status"]
        if service in self.down:
            expected = ("exited",)
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
        now = _now()
        record = {"t": now, "offset": self.offset(now)}
        if "victoria-metrics" in self.down or "victoria-metrics" in self.paused:
            record["skipped"] = "victoria-metrics held by a step"
        elif not self.vm_base:
            record["error"] = "no published port"
        else:
            try:
                record.update(vm.freshness(self.vm_base, self.scenario.ledger["vm_selector"],
                                           now, FRESHNESS_LOOKBACK_S))
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

        result = self.docker.run(["stop", "-t", str(GENERATOR_STOP_T), self.ids["generator"]],
                                 timeout=GENERATOR_STOP_T + 15)
        self.phase("generator_stopped", rc=result.rc, stderr=result.stderr.strip())

        if self.external:
            self.wait_sink_drained()
        else:
            self.wait_quiet()

        result = self.docker.run(["stop", "-t", str(SUT_STOP_T), self.ids["logit"]],
                                 timeout=SUT_STOP_T + 15)
        info = self.docker.inspect(self.ids["logit"]) or {}
        self.phase("sut_stopped", rc=result.rc, stderr=result.stderr.strip(),
                   exit_code=(info.get("State") or {}).get("ExitCode"))

        if self.external:
            self.phase("end_end")
            return
        self.refresh_vm_base()
        export_path = self.run_dir / "vm-export.jsonl"
        try:
            vm.force_flush(self.vm_base)
            text = vm.export_raw(self.vm_base, self.scenario.ledger["vm_selector"], 0,
                                 timeout=120)
            export_path.write_text(text)
            self.phase("vm_exported", series=len(vm.parse_export(text)))
        except (vm.VmError, TypeError) as err:
            export_path.write_text("")
            self.phase("vm_export_failed", error=str(err))
        self.phase("end_end")

    def wait_quiet(self):
        """Waits until VictoriaMetrics' total holds for two aggregate windows, bounded at
        `QUIET_BOUND_S`. The window is the stored samples' spacing: `aggregate` writes each
        series once per window while it has data, and stops writing an idle one, so newer
        timestamps can't mark the windows once the generator has stopped."""
        self.refresh_vm_base()
        deadline = _now() + QUIET_BOUND_S
        selector = self.scenario.ledger["vm_selector"]
        last_total = None
        since = _now()
        hold_s = None
        polls = 0
        while _now() < deadline:
            polls += 1
            try:
                vm.force_flush(self.vm_base)
                series = vm.export(self.vm_base, selector, int(_now() - 300))
            except (vm.VmError, TypeError):
                time.sleep(2)
                continue
            if hold_s is None:
                window = vm.sample_spacing_s(series)
                hold_s = 2 * window if window else None
            total = vm.last_total(series)
            if total != last_total:
                last_total = total
                since = _now()
            elif hold_s is not None and _now() - since >= hold_s:
                self.phase("quiet", held=True, total=total, hold_s=hold_s, polls=polls)
                return
            time.sleep(2)
        self.phase("quiet", held=False, total=last_total, hold_s=hold_s, polls=polls,
                   bound_s=QUIET_BOUND_S)

    def aggregate_interval(self):
        """The SUT config's `aggregate` window in seconds, or `DEFAULT_AGGREGATE_INTERVAL_S`."""
        try:
            text = self.scenario.config_path("sut").read_text()
        except OSError:
            text = ""
        interval = checks.component_duration(
            text, self.scenario.ledger.get("sut_aggregate", ""), "interval")
        return interval or DEFAULT_AGGREGATE_INTERVAL_S

    def wait_sink_drained(self):
        """For an external target, which nothing queries: waits until a SUT drain at least two
        aggregate windows after the generator stopped shows the sink's `buffer.batches` at 0
        and its `retrying` at 0 or never set, bounded at `SINK_DRAIN_BOUND_S`. Two windows let
        `aggregate` flush the last lines the generator sent; the sink's queue then empties once
        its last send is accepted. Reads the newest `SINK_DRAIN_TAIL_LINES` of the SUT's
        stdout, which a run in progress hasn't collected yet."""
        sink = self.scenario.ledger["sut_sink"]
        stopped = _now()
        hold_s = 2 * self.aggregate_interval()
        deadline = stopped + SINK_DRAIN_BOUND_S
        polls = 0
        state = {}
        while _now() < deadline:
            polls += 1
            result = self.docker.logs_tail(self.ids["logit"], SINK_DRAIN_TAIL_LINES,
                                           timeout=POLL_TIMEOUT_S)
            tel = telemetry.parse_ndjson(result.stdout.splitlines())
            newest = max(tel.drains, default=None)
            queued = tel.gauge_series("logit.component.buffer.batches", component=sink)
            retrying = tel.gauge_series("logit.component.retrying", component=sink)
            state = {
                "newest_drain": newest,
                "buffer_batches": queued[-1][1] if queued else None,
                "retrying": retrying[-1][1] if retrying else None,
            }
            if (newest is not None and newest >= stopped + hold_s
                    and state["buffer_batches"] == 0 and not state["retrying"]):
                self.phase("sink_drained", held=True, hold_s=hold_s, polls=polls,
                           waited_s=round(_now() - stopped, 3), **state)
                return
            time.sleep(2)
        self.phase("sink_drained", held=False, hold_s=hold_s, polls=polls,
                   bound_s=SINK_DRAIN_BOUND_S, **state)
        self.say(f"the {sink} queue didn't empty within {SINK_DRAIN_BOUND_S}s: {state}")

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
    run = Run(root, scenario, duration, seed, keep, out_dir, argv, image, external_env)
    try:
        run.prepare()
    except BaseException:
        scenario_mod.remove_private_env(run.private_env)
        raise
    return run.execute()
