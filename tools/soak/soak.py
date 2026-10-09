#!/usr/bin/env python3
"""The `script/soak` driver's command line: `run`, `list`, `check`, and `self-test`.

`script/soak` builds the images and validates a scenario's `logit` configs before it runs this,
so run it through that script; `list`, `check`, and `self-test` need no Docker and can run
directly. Standard library only, Python 3.11 or later. See README.md here.

Environment: `DOCKER` (the command prefix, `sudo docker` by default), `SOAK_OUT` (where run
directories go, default `perf/results/soak`), and `SOAK_IMAGE` (the `logit` image tag,
`logit:soak`).
"""

import argparse
import os
import sys
from pathlib import Path

if sys.version_info < (3, 11):
    sys.exit("soak: needs Python 3.11 or later (tomllib)")

TOOLS = Path(__file__).resolve().parent
sys.path.insert(0, str(TOOLS))

from soaklib import checks, driver, report, scenario, selftest  # noqa: E402

ROOT = TOOLS.parent.parent


def scenario_path(name):
    path = Path(name)
    if path.suffix == ".toml" and path.is_file():
        return path
    if path.is_dir() and (path / "scenario.toml").is_file():
        return path / "scenario.toml"
    candidate = TOOLS / "scenarios" / name / "scenario.toml"
    if candidate.is_file():
        return candidate
    names = ", ".join(p.parent.name for p in scenario.shipped(TOOLS))
    sys.exit(f"soak: no scenario {name!r} (shipped: {names})")


def cmd_run(args):
    if selftest.run(TOOLS, quiet=True) != 0:
        print("soak: self-test failed; not running", file=sys.stderr)
        return 1
    try:
        duration = None if args.duration is None else scenario.parse_duration(args.duration)
        out = Path(args.out or os.environ.get("SOAK_OUT") or ROOT / "perf/results/soak")
        return driver.run(
            TOOLS, scenario_path(args.scenario), duration, args.seed, args.keep, out.resolve(),
            sys.argv, os.environ.get("SOAK_IMAGE", "logit:soak"),
        )
    except (ValueError, scenario.ScenarioError) as err:
        print(f"soak: {err}", file=sys.stderr)
        return 2


def cmd_list(args):
    for path in scenario.shipped(TOOLS):
        try:
            loaded = scenario.load(path)
        except scenario.ScenarioError as err:
            print(f"{path.parent.name}\tINVALID: {err}")
            continue
        if loaded.random is not None:
            schedule = (f"random, seed {loaded.random.seed}, "
                        f"{len(scenario.expand(loaded))} fault(s)")
        else:
            schedule = f"{len(loaded.steps)} step(s)"
        print(f"{loaded.name}\t{scenario.format_duration(loaded.duration)}\t"
              f"{schedule}\t{loaded.description}")
    return 0


def cmd_check(args):
    run_dir = Path(args.run_dir)
    if not (run_dir / "timeline.jsonl").is_file():
        print(f"soak: {run_dir} has no timeline.jsonl", file=sys.stderr)
        return 2
    results = checks.run_all(run_dir)
    report.write(run_dir, results)
    print(report.markdown_table(results))
    return driver.exit_code(results)


def cmd_self_test(args):
    return selftest.run(TOOLS)


def main(argv=None):
    parser = argparse.ArgumentParser(prog="script/soak", description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="run a scenario and score it")
    run.add_argument("scenario", help="a name under tools/soak/scenarios/, or a path")
    run.add_argument("--duration", help="override the scenario's duration, such as 20m")
    run.add_argument("--seed", type=int,
                     help="draw a [random] scenario's schedule with this seed instead of its "
                          "own; refused for a fixed schedule")
    run.add_argument("--keep", action="store_true", help="leave the compose project up")
    run.add_argument("--out", help="where run directories go (default perf/results/soak)")
    run.set_defaults(func=cmd_run)

    sub.add_parser("list", help="list the shipped scenarios").set_defaults(func=cmd_list)

    check = sub.add_parser("check", help="re-score a run directory offline")
    check.add_argument("run_dir")
    check.set_defaults(func=cmd_check)

    sub.add_parser("self-test", help="test the driver's pure parts").set_defaults(
        func=cmd_self_test)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
