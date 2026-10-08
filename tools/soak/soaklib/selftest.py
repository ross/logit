"""`soak.py self-test`: the driver's pure parts against embedded fixtures, plus `validate()` over
every shipped scenario. `run` calls it first, so a broken reducer or rule can't score a run.

It covers the duration parser, every `validate()` rejection rule, `expand()`'s cycle
repetition, the NDJSON reducers (counters per life split where uptime decreases, gauge series),
the stderr classifier (a raw `thread '...' panicked at` line included), the slope function, and
the checks against small synthetic run directories: a failed fault action, an aborted run and its
exit code, an fd sample inside a later fault, and a steady state too thin to judge.
"""

import copy
import json
import tempfile
import tomllib
from datetime import datetime, timezone
from pathlib import Path

from . import checks, driver, scenario, telemetry

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

    _refused(path, mutate(drop_for), "every fault needs `for`", "a step without for")
    _refused(path, mutate(overlap), "two faults overlap on generator", "overlapping faults")
    _refused(path, mutate(netem_in_stop), "netem on victoria-metrics during its stop",
             "netem during a stop")
    _refused(path, mutate(lambda r: r.update(cooldown="30s")), "shorter than recovery_bound",
             "cooldown under recovery_bound")
    _refused(path, mutate(lambda r: r["step"][0].update(at="12m")), "after the 13m cycle",
             "a step past its cycle")
    _refused(path, mutate(lambda r: r["step"][0].update(action="kill")), "unknown action",
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
    _refused(path, copy.deepcopy(base), "W4", "--seed", seed=7)
    _refused(path, copy.deepcopy(base), "leaves no time", "a duration inside warmup+cooldown",
             duration=150)


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


def _line(ts, name, kind, value, component="self", **fields):
    metric = {"name": name, "kind": kind}
    if kind == "sum":
        metric.update(value=value, temporality="delta", monotonic=True)
    else:
        metric["value"] = value
    metric.update(fields)
    return ('{"timestamp":"%s","metrics":[%s],"attributes":{"component":"%s","kind":"x",'
            '"role":"sink"}}' % (ts, json.dumps(metric), component))


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
    for part in (_durations, _rules, _expand, _ndjson, _stderr, _slope, _checks, _shipped):
        try:
            part(root) if part in (_rules, _expand, _shipped) else part()
        except Exception as err:  # report the part that broke, then keep going
            _FAILURES.append(f"{part.__name__}: {type(err).__name__}: {err}")
    if _FAILURES:
        for failure in _FAILURES:
            print(f"self-test FAIL: {failure}")
        return 1
    if not quiet:
        print(f"self-test: {_PASSED[0]} expectations passed")
    return 0
