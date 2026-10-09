"""Writes a run's `results.md` (a table, then each check's detail lines) and `results.json`
(the same, machine-readable, with the overall status)."""

import json
from pathlib import Path

from .checks import FAIL, worst


def markdown_table(results):
    rows = ["| Check | Status | Detail |", "|---|---|---|"]
    for result in results:
        detail = result.detail.replace("|", "\\|")
        rows.append(f"| `{result.id}` | {result.status} | {detail} |")
    return "\n".join(rows)


def _header(run_dir):
    try:
        resolved = json.loads((Path(run_dir) / "scenario.resolved.json").read_text())
    except (OSError, ValueError):
        resolved = {}
    return resolved.get("name", "?"), resolved.get("duration"), resolved.get("seed")


def write(run_dir, results):
    run_dir = Path(run_dir)
    name, duration, seed = _header(run_dir)
    overall = worst([result.status for result in results])
    scheduled = "" if duration is None else f", {duration:g}s scheduled"
    seeded = "" if seed is None else f", random schedule seed {seed}"
    lines = [
        f"# soak: {name}",
        "",
        f"Run `{run_dir.name}`{scheduled}{seeded}. Overall: **{overall}**.",
        "",
        markdown_table(results),
        "",
        "## Details",
    ]
    for result in results:
        lines += ["", f"### `{result.id}`: {result.status}", "", result.detail]
        if result.lines:
            lines.append("")
            lines += [f"- {line}" for line in result.lines]
    (run_dir / "results.md").write_text("\n".join(lines) + "\n")
    (run_dir / "results.json").write_text(json.dumps({
        "scenario": name,
        "run": run_dir.name,
        "seed": seed,
        "overall": overall,
        "failed": overall == FAIL,
        "checks": [result.to_json() for result in results],
    }, indent=2) + "\n")
