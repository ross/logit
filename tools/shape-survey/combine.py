#!/usr/bin/env python3
"""Fold several `shape-survey` run directories into one cross-producer markdown report.

One table per dimension, one row per producer x source x tap x signal; docs/design/data-shapes.md
is written from this output. Input is N run directories, each with `summary.json` and
`provenance.txt`. Nothing re-parses `shape.log`: `summary.json`'s value->count tables make any
fraction recomputable here.

Every row carries its run's representativeness line, because these tables get quoted row by row.

What it emits:

  * one table per distributed dimension (`attributes` -- with the >4/>8/>12/>16 fractions the
    `AttrMap` sizing question turns on -- `batch.events`, `batch.resource_attributes`,
    `batch.scope_attributes`, `batch.keysets`, `nested_maps`, `nested_map_width`, `value_depth`,
    `key_bytes`, `value_bytes`, `body_bytes`, `metrics`, `samples_per_metric`,
    `span_events`, `span_event_attributes`, `span_links`, and anything else a capture contains),
  * the `values.*` type mix as percentages of all values observed,
  * the counters, and
  * the cumulative gauges (`distinct_keys`, `distinct_keysets`, `keyset_share.top1`/`top5`,
    `tracking_overflow`) pivoted one row per source x tap.

Stdlib only.

Usage:
    combine.py perf/results/shape-survey/*/*/ --out combined.md
    combine.py --self-test
"""

import argparse
import json
import pathlib
import sys
import tempfile
from collections import OrderedDict

#: Table order: width, grouping, value contents, then signal-specific. A dimension not listed still
#: gets a table, after these.
DIMENSION_ORDER = (
    "logit.shape.attributes",
    "logit.shape.batch.events",
    "logit.shape.batch.resource_attributes",
    "logit.shape.batch.scope_attributes",
    "logit.shape.batch.keysets",
    "logit.shape.key_bytes",
    "logit.shape.value_bytes",
    "logit.shape.body_bytes",
    "logit.shape.value_depth",
    "logit.shape.nested_maps",
    "logit.shape.nested_map_width",
    "logit.shape.metrics",
    "logit.shape.samples_per_metric",
    "logit.shape.span_events",
    "logit.shape.span_event_attributes",
    "logit.shape.span_links",
)

#: The `logit.shape.attributes` table adds the spill fraction at each candidate inline capacity.
WIDTH_THRESHOLDS = (4, 8, 12, 16)

#: The cumulative gauges, pivoted into columns in this order; a missing one prints `n/a`.
GAUGES = (
    "logit.shape.distinct_keys",
    "logit.shape.distinct_keysets",
    "logit.shape.keyset_share.top1",
    "logit.shape.keyset_share.top5",
    "logit.shape.tracking_overflow",
)

VALUES_PREFIX = "logit.shape.values."


# ---- loading ---------------------------------------------------------------------------------


class Run:
    """One run directory: its summary, and who it is."""

    def __init__(self, path: pathlib.Path) -> None:
        self.path = path
        summary_file = path / "summary.json"
        if not summary_file.is_file():
            raise SystemExit(f"combine: {path} has no summary.json -- is it a shape-survey run dir?")
        self.summary = json.loads(summary_file.read_text())
        # summary.json already carries the banner fields; provenance.txt is read only for the
        # producer's software versions, reported as a per-run appendix.
        self.provenance = ""
        if (path / "provenance.txt").is_file():
            self.provenance = (path / "provenance.txt").read_text()
        self.producer = self.summary.get("producer") or path.parent.name
        self.captured = self.summary.get("captured", "?")
        self.representativeness = self.summary.get("representativeness") or "NOT STATED"
        #: Distinct within one report even when a producer was captured twice.
        self.label = self.producer

    def rows(self, section: str):
        return self.summary.get(section) or []


def label_runs(runs: list[Run]) -> None:
    """Disambiguates two runs of the same producer as `demo`, `demo#2`, ... in row labels."""
    seen: dict[str, int] = {}
    for run in runs:
        seen[run.producer] = seen.get(run.producer, 0) + 1
        if seen[run.producer] > 1:
            run.label = f"{run.producer}#{seen[run.producer]}"


# ---- statistics recomputed from the value->count tables ----------------------------------------


def fractions_above(histogram: dict, thresholds=WIDTH_THRESHOLDS) -> dict[int, float] | None:
    """The fraction of observations strictly above each threshold, from a value->count table."""
    if not histogram:
        return None
    total = sum(histogram.values())
    if not total:
        return None
    return {
        t: sum(c for v, c in histogram.items() if int(v) > t) / total for t in thresholds
    }


def fmt(value) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.0f}" if float(value).is_integer() else f"{value:.2f}"
    return str(value)


def pct(value) -> str:
    return "n/a" if value is None else f"{value:.1%}"


# ---- rendering -------------------------------------------------------------------------------


def dimension_table(runs: list[Run], metric: str) -> list[str]:
    widths = metric == "logit.shape.attributes"
    header = "| producer | source | tap | signal | n | min | p50 | p90 | p99 | max | mean |"
    rule = "|---|---|---|---|--:|--:|--:|--:|--:|--:|--:|"
    if widths:
        header += " >4 | >8 | >12 | >16 |"
        rule += "--:|--:|--:|--:|"
    header += " representativeness |"
    rule += "---|"

    body: list[str] = []
    for run in runs:
        for row in run.rows("distributions"):
            if row["metric"] != metric:
                continue
            s = row["stats"]
            line = (
                f"| {run.label} | {row['source'] or '-'} | {row['tap'] or '-'} |"
                f" {row['signal'] or '-'} | {s['count']} | {fmt(s['min'])} | {fmt(s['p50'])} |"
                f" {fmt(s['p90'])} | {fmt(s['p99'])} | {fmt(s['max'])} | {fmt(s['mean'])} |"
            )
            if widths:
                f = fractions_above(s.get("histogram") or {})
                for t in WIDTH_THRESHOLDS:
                    line += f" {pct(f[t] if f else None)} |"
            line += f" {run.representativeness} |"
            body.append(line)
    if not body:
        return []
    return [f"## `{metric}`", "", header, rule, *body, ""]


def values_table(runs: list[Run]) -> list[str]:
    """The `values.*` counters as percentages of all values observed, per source x tap x signal."""
    kinds: list[str] = []
    rows: "OrderedDict[tuple, dict]" = OrderedDict()
    for run in runs:
        for row in run.rows("counters"):
            if not row["metric"].startswith(VALUES_PREFIX):
                continue
            kind = row["metric"][len(VALUES_PREFIX) :]
            if kind not in kinds:
                kinds.append(kind)
            key = (run.label, row["source"], row["tap"], row["signal"], run.representativeness)
            rows.setdefault(key, {})[kind] = row["total"]
    if not rows:
        return []
    kinds.sort()
    out = [
        "## `logit.shape.values.*` — the type mix",
        "",
        "Percentages of every attribute value observed at that tap (a map or array counts once as"
        " itself, and its members are counted too, so a nested event contributes more values than"
        " it has top-level attributes).",
        "",
        "| producer | source | tap | signal | values | "
        + " | ".join(f"{k} %" for k in kinds)
        + " | representativeness |",
        "|---|---|---|---|--:|" + "--:|" * len(kinds) + "---|",
    ]
    for (label, source, tap, signal, note), counts in rows.items():
        total = sum(counts.values())
        cells = " ".join(
            f"{(counts.get(k, 0.0) / total):.1%} |" if total else "n/a |" for k in kinds
        )
        out.append(
            f"| {label} | {source or '-'} | {tap or '-'} | {signal or '-'} | {fmt(total)} |"
            f" {cells} {note} |"
        )
    out.append("")
    return out


def counters_table(runs: list[Run]) -> list[str]:
    body = []
    for run in runs:
        for row in run.rows("counters"):
            if row["metric"].startswith(VALUES_PREFIX):
                continue
            body.append(
                f"| {run.label} | `{row['metric']}` | {row['source'] or '-'} | {row['tap'] or '-'} |"
                f" {row['signal'] or '-'} | {fmt(row['total'])} |"
            )
    if not body:
        return []
    return [
        "## Counters",
        "",
        "| producer | metric | source | tap | signal | total |",
        "|---|---|---|---|---|--:|",
        *body,
        "",
    ]


def gauges_table(runs: list[Run]) -> list[str]:
    pivot: "OrderedDict[tuple, dict]" = OrderedDict()
    for run in runs:
        for row in run.rows("gauges"):
            key = (run.label, row["source"], row["tap"], run.representativeness)
            pivot.setdefault(key, {})[row["metric"]] = row["value"]
    if not pivot:
        return []
    out = [
        "## Cumulative gauges (last value in each capture)",
        "",
        "Cumulative since process start, not windowed, and over top-level keys only"
        " (`docs/known-gaps.md`). `tracking_overflow` at `1` means a cap was hit and that row's"
        " key/key-set numbers are floors.",
        "",
        "| producer | source | tap | " + " | ".join(g.rsplit(".", 1)[-1] for g in GAUGES)
        + " | representativeness |",
        "|---|---|---|" + "--:|" * len(GAUGES) + "---|",
    ]
    for (label, source, tap, note), values in pivot.items():
        cells = " ".join(f"{fmt(values.get(g))} |" for g in GAUGES)
        out.append(f"| {label} | {source or '-'} | {tap or '-'} | {cells} {note} |")
    out.append("")
    return out


def render(runs: list[Run]) -> str:
    out: list[str] = ["# Event shapes across every survey run", ""]
    out.append(
        "One row per producer x source x tap x signal, one table per dimension, every row"
        " carrying its run's representativeness line. Percentiles are **nearest-rank** over the"
        " whole capture and are withheld (`n/a`) below the value counts `summarize.py` states;"
        " the plan's own rule is stricter for anything quoted in"
        " `docs/design/data-shapes.md` (a p99 needs >=100k events behind it)."
    )
    out.append("")
    out.append("## Runs")
    out.append("")
    out.append("| producer | captured | directory | representativeness |")
    out.append("|---|---|---|---|")
    for run in runs:
        out.append(f"| {run.label} | {run.captured} | `{run.path}` | {run.representativeness} |")
    out.append("")

    metrics: list[str] = [m for m in DIMENSION_ORDER]
    for run in runs:
        for row in run.rows("distributions"):
            if row["metric"] not in metrics:
                metrics.append(row["metric"])
    for metric in metrics:
        out += dimension_table(runs, metric)

    out += values_table(runs)
    out += counters_table(runs)
    out += gauges_table(runs)

    out.append("## Provenance, per run")
    out.append("")
    for run in runs:
        out.append(f"<details><summary><code>{run.path}</code></summary>")
        out.append("")
        out.append("```")
        out.append(run.provenance.rstrip("\n"))
        out.append("```")
        out.append("")
        out.append("</details>")
        out.append("")
    return "\n".join(out) + "\n"


# ---- self-test -------------------------------------------------------------------------------


def self_test() -> None:
    """Two synthetic run directories through the whole path, checked on the numbers it recomputes."""
    with tempfile.TemporaryDirectory() as tmp:
        root = pathlib.Path(tmp)
        first = root / "interop" / "20260920T000000Z"
        second = root / "exporters" / "20260920T010000Z"
        for path, producer, note, hist in (
            (first, "interop", "recorded real producers", {"0": 1, "7": 2, "9": 1}),
            (second, "exporters", "official exporters, idle", {"2": 6, "5": 4}),
        ):
            path.mkdir(parents=True)
            summary = {
                "producer": producer,
                "captured": "2026-09-20T00:00:00Z",
                "representativeness": note,
                "distributions": [
                    {
                        "metric": "logit.shape.attributes",
                        "signal": "metric",
                        "source": "src",
                        "tap": "tap_input",
                        "stats": {
                            "count": sum(hist.values()),
                            "min": 0,
                            "max": 9,
                            "mean": 5.0,
                            "p50": 7,
                            "p90": None,
                            "p99": None,
                            "histogram": hist,
                        },
                    }
                ],
                "counters": [
                    {
                        "metric": "logit.shape.values.string",
                        "signal": "metric",
                        "source": "src",
                        "tap": "tap_input",
                        "total": 30.0,
                    },
                    {
                        "metric": "logit.shape.values.int",
                        "signal": "metric",
                        "source": "src",
                        "tap": "tap_input",
                        "total": 10.0,
                    },
                    {
                        "metric": "logit.shape.events",
                        "signal": "metric",
                        "source": "src",
                        "tap": "tap_input",
                        "total": 4.0,
                    },
                ],
                "gauges": [
                    {
                        "metric": "logit.shape.distinct_keys",
                        "signal": "",
                        "source": "",
                        "tap": "tap_input",
                        "value": 9.0,
                    }
                ],
            }
            (path / "summary.json").write_text(json.dumps(summary))
            (path / "provenance.txt").write_text(f"producer: {producer}\n")

        runs = [Run(first), Run(second)]
        label_runs(runs)
        markdown = render(runs)

        # Two runs of one producer stay distinguishable.
        doubled = [Run(first), Run(first)]
        label_runs(doubled)
        assert [r.label for r in doubled] == ["interop", "interop#2"], [r.label for r in doubled]

    # The fractions are recomputed from the value->count table, not copied: 3 of 4 interop
    # observations are >4 and 1 of 4 is >8; none of the exporters' are above either.
    fractions = fractions_above({"0": 1, "7": 2, "9": 1})
    assert fractions == {4: 0.75, 8: 0.25, 12: 0.0, 16: 0.0}, fractions
    assert "75.0% | 25.0%" in markdown, markdown
    # Both producers on the same table, each carrying its own representativeness line.
    attrs = markdown[markdown.index("## `logit.shape.attributes`") :]
    attrs = attrs[: attrs.index("\n## ", 1)]
    assert "| interop |" in attrs and "| exporters |" in attrs, attrs[:600]
    assert attrs.count("recorded real producers") == 1 and attrs.count("official exporters") == 1
    # The type mix is a percentage of all values, and `values.*` stays out of the counters table.
    assert "75.0% | 25.0%" in markdown and "logit.shape.events" in markdown
    assert "`logit.shape.values.string`" not in markdown, "values.* belongs in the mix table only"
    # A gauge a capture never produced prints n/a rather than a zero that looks measured.
    assert "| 9 | n/a |" in markdown, markdown[markdown.index("## Cumulative") :][:400]

    print("combine: self-test passed")


# ---- entry point -----------------------------------------------------------------------------


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("run_dirs", nargs="*", help="shape-survey run directories (with summary.json)")
    ap.add_argument("--out", help="write the markdown here instead of stdout")
    ap.add_argument("--self-test", action="store_true", help="check the recomputation and rendering")
    args = ap.parse_args()

    if args.self_test:
        self_test()
        return
    if not args.run_dirs:
        ap.error("give at least one run directory, or --self-test")

    runs = [Run(pathlib.Path(d)) for d in args.run_dirs]
    runs.sort(key=lambda r: (r.producer, r.captured))
    label_runs(runs)
    markdown = render(runs)
    if args.out:
        pathlib.Path(args.out).write_text(markdown)
        print(f"combine: wrote {args.out} from {len(runs)} run(s)", file=sys.stderr)
    else:
        sys.stdout.write(markdown)


if __name__ == "__main__":
    main()
