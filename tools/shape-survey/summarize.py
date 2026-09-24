#!/usr/bin/env python3
"""Fold a `shape.log` capture into `summary.json` + `summary.md`, for `script/shape-survey`.

Parses `stdio_out`/`file_out`'s human render (`crates/logit-outputs/src/stdio.rs`), the only text
output in `logit` that emits raw metric values rather than re-sketched quantiles.

What it does with each `logit.shape.*` record, per series (a series being the metric name plus the
`signal`/`source`/`tap` tags `shape` stamps):

  samples=[...]  concatenated across every flush window: raw observations, one per event (or
                 per batch), so the summary covers the whole population
  sum=           summed (a counter, delta temporality from `aggregate`)
  gauge=         last value wins (cumulative-since-start gauges: distinct keys, key-set shares)

A sketched `logit.shape.*` series is a hard failure. Past `max_samples_per_series`, `aggregate`
falls back to a `DdSketch` (`distribution count=N p50=...`), which would make every distribution
silently approximate. This exits non-zero and names the series, so the config's cap gets raised.

Percentiles are nearest-rank. A p99 prints `n/a` below 100 values and a p90 below 10, and every
table carries its own value count.

Every summary opens with the representativeness banner from `provenance.txt` (README
"Representativeness is structural"). `--source-labels` adds a per-source table of where each
source's format came from. Stdlib only.

Usage:
    summarize.py --shape-log /out/shape.log --out-dir /out --provenance /out/provenance.txt
    summarize.py --self-test        # parse an embedded sample of the real render, check the numbers
"""

import argparse
import json
import math
import pathlib
import sys
from collections import Counter, OrderedDict

#: The tags `shape` stamps on everything it emits (docs/adr/shape-observer-component.md). A series
#: is identified by its metric name plus these, in this order.
TAG_KEYS = ("signal", "source", "tap")

#: Attribute-count thresholds around `AttrMap`'s inline capacity of 8 (docs/design/memory.md,
#: "Recommendations"). Reported as "fraction of events wider than this".
WIDTH_THRESHOLDS = (4, 8, 12, 16)

#: Below this many values a p99 is not printed at all, and below `MIN_FOR_P90` a p90 is not.
MIN_FOR_P99 = 100
MIN_FOR_P90 = 10


# ---- parsing -------------------------------------------------------------------------------------


def parse_quoted(text: str, i: int) -> tuple[str, int]:
    """Reads one `"..."` token starting at `text[i]`, undoing `render_quoted_str`'s escapes."""
    assert text[i] == '"'
    out: list[str] = []
    i += 1
    while i < len(text):
        c = text[i]
        if c == '"':
            return "".join(out), i + 1
        if c == "\\":
            nxt = text[i + 1]
            if nxt == "x":
                out.append(chr(int(text[i + 2 : i + 4], 16)))
                i += 4
                continue
            out.append({"n": "\n", "r": "\r", "t": "\t"}.get(nxt, nxt))
            i += 2
            continue
        out.append(c)
        i += 1
    raise ValueError(f"unterminated quoted string in {text!r}")


def skip_container(text: str, i: int) -> int:
    """Returns the index just past the `[...]`/`{...}` that starts at `text[i]`.

    `render_value` renders a `Value::Array` as `[a, b]` and a `Value::Map` as `{k=v, k=v}`,
    recursively, and either can hold a quoted string containing a bracket, brace, comma, or space.
    So this walks the container, counting nesting and stepping over quoted strings whole (a `\\"`
    inside one isn't its terminator).
    """
    closers = {"[": "]", "{": "}"}
    stack = [closers[text[i]]]
    i += 1
    while i < len(text):
        c = text[i]
        if c == '"':
            _, i = parse_quoted(text, i)
            continue
        if c in closers:
            stack.append(closers[c])
            i += 1
            continue
        if c == stack[-1]:
            stack.pop()
            i += 1
            if not stack:
                return i
            continue
        i += 1
    raise ValueError(f"unterminated container in {text!r}")


def parse_value(text: str, i: int) -> tuple[str, int]:
    """Reads one rendered `Value` starting at `text[i]` (`crates/logit-outputs/src/stdio.rs`).

    Every variant `render_value` can emit, and how each one ends:

    | rendered                | variant                     | ends at                     |
    |-------------------------|-----------------------------|-----------------------------|
    | `"..."`                 | `Str`                       | the closing quote           |
    | `[a, b]` / `{k=v}`      | `Array` / `Map`             | the matching bracket/brace  |
    | `<12 bytes>`            | `Bytes`                     | the `>`                     |
    | `null`/`true`/`-1`/`.5` | `Null`/`Bool`/ints/floats   | the next space              |
    | `2026-09-20T…Z`         | `Timestamp`                 | the next space              |

    A string is returned decoded; a container as its raw rendered text, since nothing under
    `TAG_KEYS` is a container. The three space-bearing forms (a quoted string, a container, and
    `<N bytes>`) are why an `attrs` line can't be split on whitespace.
    """
    c = text[i]
    if c == '"':
        return parse_quoted(text, i)
    if c in "[{":
        end = skip_container(text, i)
        return text[i:end], end
    if c == "<":
        # `Value::Bytes` -> `<12 bytes>`: bare, and it contains a space.
        end = text.find(">", i)
        if end < 0:
            raise ValueError(f"unterminated <N bytes> in {text!r}")
        return text[i : end + 1], end + 1
    end = text.find(" ", i)
    end = len(text) if end < 0 else end
    return text[i:end], end


def parse_attrs(text: str) -> dict[str, str]:
    """Parses one `attrs` line's space-separated `key=value` pairs.

    `render_attrs`/`render_merged_attrs` write space-separated pairs, the key through `render_key`
    (bare when identifier-shaped, quoted otherwise) and the value through `render_value`, so both
    sides can contain spaces, `=`, commas, brackets, and quotes (see `parse_value`).

    Only `TAG_KEYS` are read back out; the other pairs are parsed only to find where the next one
    starts.
    """
    attrs: dict[str, str] = {}
    i = 0
    while i < len(text):
        if text[i] == " ":
            i += 1
            continue
        if text[i] == '"':
            key, i = parse_quoted(text, i)
        else:
            end = text.find("=", i)
            if end < 0:
                raise ValueError(f"no = after key at {i} in {text!r}")
            key, i = text[i:end], end
        if i >= len(text) or text[i] != "=":
            raise ValueError(f"expected = at {i} in {text!r}")
        i += 1
        if i >= len(text):
            raise ValueError(f"key {key!r} has no value in {text!r}")
        attrs[key], i = parse_value(text, i)
    return attrs


class Series:
    """One metric name + tag set, and everything the capture said about it."""

    def __init__(self, name: str, tags: dict[str, str]) -> None:
        self.name = name
        self.tags = tags
        self.samples: list[float] = []
        self.total: float | None = None
        self.gauge: float | None = None

    def key(self) -> tuple:
        return (self.name, *(self.tags.get(k, "") for k in TAG_KEYS))


def parse(text: str) -> "OrderedDict[tuple, Series]":
    """Parses a whole `shape.log` into series.

    A block starts at any line with no leading whitespace (`render_event_block` writes the
    timestamp flush left and indents everything else). `attrs` carries the block's tags; every
    `metric` line in the block belongs to them.
    """
    series: OrderedDict[tuple, Series] = OrderedDict()
    tags: dict[str, str] = {}
    sketched: list[str] = []

    for raw in text.splitlines():
        if not raw or not raw.startswith(" "):
            tags = {}
            continue
        line = raw.strip()
        if line.startswith("attrs "):
            tags = parse_attrs(line[len("attrs ") :].strip())
            continue
        if not line.startswith("metric "):
            continue
        body = line[len("metric ") :].strip()
        name, _, rendered = body.partition(" ")
        if not name.startswith("logit.shape."):
            # Skipped, not an error: a producer may point other traffic at the same file_out.
            continue
        entry = series.setdefault((name, *(tags.get(k, "") for k in TAG_KEYS)), Series(name, tags))

        if rendered.startswith("samples=["):
            values = rendered[len("samples=[") : rendered.index("]")]
            if values:
                entry.samples.extend(float(v) for v in values.split(","))
        elif rendered.startswith("sum="):
            entry.total = (entry.total or 0.0) + float(rendered[len("sum=") :].split(" ")[0])
        elif rendered.startswith("gauge="):
            entry.gauge = float(rendered[len("gauge=") :].split(" ")[0])
        elif rendered.startswith("distribution "):
            sketched.append(f"{name}{sorted(tags.items())}")
        else:
            raise ValueError(f"unrecognized metric render: {body!r}")

    if sketched:
        raise SystemExit(
            "summarize: a logit.shape.* series was rendered as a DdSketch, not raw samples:\n  "
            + "\n  ".join(sorted(set(sketched)))
            + "\nThat means `aggregate` fell back past `max_samples_per_series` and the values"
            " behind it are gone. Raise that limit in the config; do not summarize this capture."
        )
    return series


# ---- statistics ----------------------------------------------------------------------------------


def nearest_rank(sorted_values: list[float], q: float) -> float:
    """Nearest-rank percentile: the value at ceil(q * n) in a 1-based sorted sample."""
    rank = max(1, math.ceil(q * len(sorted_values)))
    return sorted_values[rank - 1]


def describe(values: list[float]) -> dict:
    """Count/min/max/mean/percentiles, with percentiles withheld below their own minimums."""
    ordered = sorted(values)
    n = len(ordered)
    stats: dict = {
        "count": n,
        "min": ordered[0],
        "max": ordered[-1],
        "mean": sum(ordered) / n,
        "p50": nearest_rank(ordered, 0.50),
        "p90": nearest_rank(ordered, 0.90) if n >= MIN_FOR_P90 else None,
        "p99": nearest_rank(ordered, 0.99) if n >= MIN_FOR_P99 else None,
    }
    if all(float(v).is_integer() for v in ordered):
        # An exact value->count table answers "what fraction sits at or above N" for every N.
        stats["histogram"] = {str(int(v)): c for v, c in sorted(Counter(ordered).items())}
    return stats


def width_fractions(values: list[float]) -> dict[str, float]:
    n = len(values)
    return {f">{t}": sum(1 for v in values if v > t) / n for t in WIDTH_THRESHOLDS}


def fmt(value) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.0f}" if float(value).is_integer() else f"{value:.2f}"
    return str(value)


# ---- rendering -----------------------------------------------------------------------------------


def render_markdown(summary: dict) -> str:
    out: list[str] = []
    out.append("# Event-shape survey")
    out.append("")
    # The banner comes before any number; a missing one says so rather than opening with a table.
    banner = summary.get("representativeness")
    out.append(f"> **Representativeness:** {banner}" if banner else
               "> **Representativeness: not stated** -- this run's provenance.txt carried no"
               " `representativeness:` line, so nothing here should be quoted until it does.")
    if summary.get("producer"):
        out.append(f">")
        out.append(f"> Producer: `{summary['producer']}`. Captured: {summary.get('captured', '?')}.")
    out.append("")

    labels = summary.get("source_labels") or {}
    if labels:
        out.append("## Where each source's format comes from")
        out.append("")
        out.append(
            "A source logging in **its own software's default format** is evidence about that"
            " software. A source logging in a format **this repository authored** is partly a"
            " measurement of our own choices, and is circular to the degree it is quoted as"
            " evidence about the wider world."
        )
        out.append("")
        out.append("| source | tier | format origin |")
        out.append("|---|---|---|")
        for source, label in sorted(labels.items()):
            out.append(
                f"| `{source}` | {label.get('tier', '-')} | {label.get('format', '-')} |"
            )
        out.append("")

    out.append(
        "Percentiles are **nearest-rank** over the whole capture (every flush window's raw"
        f" samples concatenated). A p99 is `n/a` below {MIN_FOR_P99} values and a p90 below"
        f" {MIN_FOR_P90}: too few observations to mean one. Every table carries its own value"
        " count."
    )
    out.append("")

    dists = summary["distributions"]
    if dists:
        out.append("## Per-event and per-batch distributions")
        out.append("")
        out.append("| metric | signal | source | tap | n | min | p50 | p90 | p99 | max | mean |")
        out.append("|---|---|---|---|--:|--:|--:|--:|--:|--:|--:|")
        for row in dists:
            s = row["stats"]
            out.append(
                f"| `{row['metric']}` | {row['signal'] or '-'} | {row['source'] or '-'} |"
                f" {row['tap'] or '-'} | {s['count']} | {fmt(s['min'])} | {fmt(s['p50'])} |"
                f" {fmt(s['p90'])} | {fmt(s['p99'])} | {fmt(s['max'])} | {fmt(s['mean'])} |"
            )
        out.append("")

    widths = summary["attribute_widths"]
    if widths:
        out.append("## Attribute width per event")
        out.append("")
        out.append(
            "The fraction of events carrying **more than** N top-level attributes --"
            " `AttrMap`'s inline capacity is 8 today (docs/design/memory.md §8), so `>8` is the"
            " spill fraction at the current constant."
        )
        out.append("")
        out.append("| signal | source | tap | events | p50 | p90 | p99 | max | >4 | >8 | >12 | >16 |")
        out.append("|---|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|")
        for row in widths:
            s, f = row["stats"], row["fractions"]
            out.append(
                f"| {row['signal'] or '-'} | {row['source'] or '-'} | {row['tap'] or '-'} |"
                f" {s['count']} | {fmt(s['p50'])} | {fmt(s['p90'])} | {fmt(s['p99'])} |"
                f" {fmt(s['max'])} | {f['>4']:.1%} | {f['>8']:.1%} | {f['>12']:.1%} |"
                f" {f['>16']:.1%} |"
            )
        out.append("")

    counters = summary["counters"]
    if counters:
        out.append("## Counters")
        out.append("")
        out.append("| metric | signal | source | tap | total |")
        out.append("|---|---|---|---|--:|")
        for row in counters:
            out.append(
                f"| `{row['metric']}` | {row['signal'] or '-'} | {row['source'] or '-'} |"
                f" {row['tap'] or '-'} | {fmt(row['total'])} |"
            )
        out.append("")

    gauges = summary["gauges"]
    if gauges:
        out.append("## Cumulative gauges (last value in the capture)")
        out.append("")
        out.append(
            "Cumulative since process start, not windowed, and over top-level keys only"
            " (docs/known-gaps.md). `keyset_share.top1`/`top5` are the share of all events"
            " carried by the most common one and five key-sets."
        )
        out.append("")
        out.append("| metric | source | tap | value |")
        out.append("|---|---|---|--:|")
        for row in gauges:
            out.append(
                f"| `{row['metric']}` | {row['source'] or '-'} | {row['tap'] or '-'} |"
                f" {fmt(row['value'])} |"
            )
        out.append("")

    # The producer's own section, last and verbatim, which keeps this file producer-agnostic.
    section = summary.get("producer_section")
    if section:
        out.append(section.rstrip("\n"))
        out.append("")

    return "\n".join(out) + "\n"


def read_provenance(path: pathlib.Path | None) -> dict:
    """The banner fields out of a run's provenance.txt: `producer`, `representativeness`, `captured`.

    A `name: value` read; the rest of the file is for a human reading the run directory.
    """
    fields: dict = {}
    if path is None or not path.is_file():
        return fields
    for line in path.read_text().splitlines():
        name, sep, value = line.partition(":")
        if sep and name in ("producer", "representativeness", "captured"):
            fields[name] = value.strip()
    return fields


def summarize(series: "OrderedDict[tuple, Series]") -> dict:
    summary: dict = {
        "percentile_method": "nearest-rank",
        "min_values_for_p90": MIN_FOR_P90,
        "min_values_for_p99": MIN_FOR_P99,
        "distributions": [],
        "attribute_widths": [],
        "counters": [],
        "gauges": [],
    }
    for entry in series.values():
        tags = {k: entry.tags.get(k, "") for k in TAG_KEYS}
        if entry.samples:
            stats = describe(entry.samples)
            summary["distributions"].append({"metric": entry.name, **tags, "stats": stats})
            if entry.name == "logit.shape.attributes":
                summary["attribute_widths"].append(
                    {**tags, "stats": stats, "fractions": width_fractions(entry.samples)}
                )
        if entry.total is not None:
            summary["counters"].append({"metric": entry.name, **tags, "total": entry.total})
        if entry.gauge is not None:
            summary["gauges"].append({"metric": entry.name, **tags, "value": entry.gauge})

    for section in ("distributions", "attribute_widths", "counters", "gauges"):
        summary[section].sort(key=lambda row: (row.get("metric", ""), row["source"], row["tap"], row["signal"]))
    return summary


# ---- self-test -----------------------------------------------------------------------------------

#: A verbatim excerpt of a real `shape.log` from the interop producer, so a change to `stdio_out`'s
#: human render fails this test. `script/shape-survey` runs `--self-test` before every survey.
SELF_TEST_RENDER = """2026-09-20T15:56:20.153724900Z
  attrs   signal="metric" source="statsd_in" tap="tap_input"
  metric  logit.shape.events sum=3 temporality=delta monotonic=true
  metric  logit.shape.attributes samples=[7,7,0] rate=1
  metric  logit.shape.value_depth samples=[0,0,0] rate=1
  metric  logit.shape.key_bytes samples=[3,11,4,8,6,20,9] rate=1
  metric  logit.shape.values.string sum=14 temporality=delta monotonic=true

2026-09-20T15:56:20.153724900Z
  attrs   source="statsd_in" tap="tap_input"
  metric  logit.shape.batch.events samples=[1,1,2] rate=1

2026-09-20T15:56:20.153724900Z
  attrs   tap="tap_input"
  metric  logit.shape.distinct_keys gauge=9
  metric  logit.shape.keyset_share.top1 gauge=0.5
  metric  logit.shape.tracking_overflow gauge=0
"""


#: A second excerpt, structurally verbatim from a real `oteldemo` `resource: keep` capture, where
#: every resource attribute lands on each record's `attrs` line. The structure is the capture's;
#: the values are neutral placeholders, because a fixture quoting a capture must not carry the
#: identity `shape`'s output never does.
#:
#: It holds what `parse_attrs` has to survive: an array of strings holding commas, spaces, and an
#: `=` (`process.command_args`, on the resource of every OTel SDK that detects a process), a bare
#: integer beside it, empty quoted strings, and a quoted string carrying spaces, `=`, and `:`.
SELF_TEST_RESOURCE_KEEP = """2026-09-20T20:43:57.818255215Z
  attrs   signal="log" source="otlp_gateway" tap="tap_input" process.pid=1 process.executable.path="/opt/app/bin/node" process.command_args=["/opt/app/bin/node", "--require=./Instrumentation.js", "/app/server.js"] process.command_line="/opt/jdk/bin/java -javaagent:/app/agent.jar -Xmx200m example.Service" host.name="host-placeholder" container.id="0000000000000000000000000000000000000000000000000000000000000000" service.name="frontend" os.description="Linux host-placeholder 0.0.0-0 #1 SMP PLACEHOLDER x86_64" host.cpu.cache.l2.size=1024 zone_name="" cluster_name=""
  metric  logit.shape.attributes samples=[11,9] rate=1
  metric  logit.shape.events sum=2 temporality=delta monotonic=true

2026-09-20T20:43:57.818255215Z
  attrs   signal="log" source="otlp_gateway" tap="tap_input" process.command_args=["./shipping"] service.name="shipping"
  metric  logit.shape.attributes samples=[7] rate=1
"""

#: The remaining `Value` variants `render_value` can put on an `attrs` line: a nested array, a
#: `Value::Map` (a `syslog_in` structured-data element) including a nested one, a `Value::Bytes`
#: (`<N bytes>`, bare, with a space in it), a key needing quotes, and a string value carrying
#: `[`, `]`, `{`, `}`, `,`, `=`, a space, and an escaped quote. Synthetic: a grammar test, not a
#: measurement.
SELF_TEST_EXOTIC_VALUES = """2026-09-20T20:43:57.818255215Z
  attrs   signal="metric" nested=[1, [2, 3], {a=1, b=[4, 5]}] sd={origin={ip="10.0.0.1", port=514}, note="a=b, c=d"} blob=<12 bytes> "odd key"="[{x=1}], \\"quoted\\"" source="syslog_in" tap="tap_input" trailing=true
  metric  logit.shape.attributes samples=[6] rate=1
"""


def self_test() -> None:
    series = parse(SELF_TEST_RENDER)

    attrs = series[("logit.shape.attributes", "metric", "statsd_in", "tap_input")]
    assert attrs.samples == [7.0, 7.0, 0.0], attrs.samples
    stats = describe(attrs.samples)
    assert stats["count"] == 3 and stats["min"] == 0.0 and stats["max"] == 7.0, stats
    # Nearest-rank p50 of [0,7,7] is the 2nd of 3 values.
    assert stats["p50"] == 7.0, stats
    assert stats["p90"] is None and stats["p99"] is None, "too few values to quote either"
    assert stats["histogram"] == {"0": 1, "7": 2}, stats["histogram"]
    fractions = width_fractions(attrs.samples)
    assert fractions[">4"] == 2 / 3 and fractions[">8"] == 0.0, fractions

    events = series[("logit.shape.events", "metric", "statsd_in", "tap_input")]
    assert events.total == 3.0, events.total

    keys = series[("logit.shape.key_bytes", "metric", "statsd_in", "tap_input")]
    assert len(keys.samples) == 7 and max(keys.samples) == 20.0, keys.samples

    batch = series[("logit.shape.batch.events", "", "statsd_in", "tap_input")]
    assert batch.samples == [1.0, 1.0, 2.0], batch.samples

    gauge = series[("logit.shape.distinct_keys", "", "", "tap_input")]
    assert gauge.gauge == 9.0, gauge.gauge
    share = series[("logit.shape.keyset_share.top1", "", "", "tap_input")]
    assert share.gauge == 0.5, share.gauge

    summary = summarize(series)
    assert len(summary["attribute_widths"]) == 1, summary["attribute_widths"]
    assert "Attribute width per event" in render_markdown(summary)

    # The banner, and the stated absence of one.
    assert "Representativeness: not stated" in render_markdown(summary)
    banner = dict(summary, representativeness="own demo stack -- harness exercise", producer="demo")
    rendered = render_markdown(banner)
    assert rendered.index("own demo stack") < rendered.index("Attribute width"), rendered[:400]

    labelled = dict(banner, source_labels={"nginx_in": {"tier": "nginx", "format": "authored here"}})
    assert "Where each source's format comes from" in render_markdown(labelled)

    # A producer's appended section lands verbatim, after the general engine's tables.
    sectioned = render_markdown(dict(banner, producer_section="## Per exporter\n\nseries per scrape\n"))
    assert sectioned.index("Attribute width") < sectioned.index("## Per exporter"), sectioned[-400:]

    # And the guard itself: a sketched shape series must stop the run, not be summarized.
    sketched = (
        "2026-09-20T15:56:20.153724900Z\n"
        '  attrs   signal="metric" source="statsd_in" tap="tap_input"\n'
        "  metric  logit.shape.attributes distribution count=4096 p50=6 p90=9 p99=12\n"
    )
    try:
        parse(sketched)
    except SystemExit as err:
        assert "DdSketch" in str(err), err
    else:
        raise AssertionError("a sketched logit.shape.* series must fail the summary")

    self_test_attrs_grammar()
    print("summarize: self-test passed")


def self_test_attrs_grammar() -> None:
    """The `attrs`-line grammar, over every `Value` variant `render_value` can emit.

    An unquoted value can contain a space: an array renders bare, `[a, b]`, and its elements can
    be quoted strings carrying an `=` (`--require=./Instrumentation.js` in
    `process.command_args`, on every OTel SDK resource that detects a process). `Value::Map` and
    `Value::Bytes` (`<12 bytes>`) have the same property. A parser that splits on the next `=`
    breaks every `resource: keep` capture.
    """
    keep = parse(SELF_TEST_RESOURCE_KEEP)

    # The whole line parses, the pairs after the array are still found, and the array's own text
    # comes back intact rather than as three garbled fragments.
    line = SELF_TEST_RESOURCE_KEEP.splitlines()[1].strip()[len("attrs ") :].strip()
    attrs = parse_attrs(line)
    assert attrs["process.command_args"] == (
        '["/opt/app/bin/node", "--require=./Instrumentation.js", "/app/server.js"]'
    ), attrs["process.command_args"]
    assert attrs["process.pid"] == "1", attrs["process.pid"]
    assert attrs["service.name"] == "frontend", attrs["service.name"]
    assert attrs["zone_name"] == "" and attrs["cluster_name"] == "", attrs
    assert attrs["process.command_line"].startswith("/opt/jdk/bin/java -javaagent:"), attrs
    assert attrs["host.cpu.cache.l2.size"] == "1024", attrs["host.cpu.cache.l2.size"]
    # And `shape`'s own three tags -- the only ones the series key is built from -- survive
    # having a dozen resource attributes, one of them an array, interleaved around them.
    assert (attrs["signal"], attrs["source"], attrs["tap"]) == ("log", "otlp_gateway", "tap_input")

    # Two blocks, same series key (the resource is not part of it, by design), so the samples
    # concatenate as they do for a `resource: drop` capture.
    kept = keep[("logit.shape.attributes", "log", "otlp_gateway", "tap_input")]
    assert kept.samples == [11.0, 9.0, 7.0], kept.samples
    assert keep[("logit.shape.events", "log", "otlp_gateway", "tap_input")].total == 2.0
    assert summarize(keep)["attribute_widths"], "a resource: keep capture must still summarize"

    # Nested containers, a map, a `<N bytes>`, a quoted key, and a string value full of the
    # delimiters. Every one of these ends where the renderer ended it, not at the next space.
    exotic = SELF_TEST_EXOTIC_VALUES.splitlines()[1].strip()[len("attrs ") :].strip()
    values = parse_attrs(exotic)
    assert values["nested"] == "[1, [2, 3], {a=1, b=[4, 5]}]", values["nested"]
    assert values["sd"] == '{origin={ip="10.0.0.1", port=514}, note="a=b, c=d"}', values["sd"]
    assert values["blob"] == "<12 bytes>", values["blob"]
    assert values["odd key"] == '[{x=1}], "quoted"', values["odd key"]
    assert values["trailing"] == "true", values["trailing"]
    assert (values["signal"], values["source"], values["tap"]) == (
        "metric",
        "syslog_in",
        "tap_input",
    ), values
    assert len(values) == 8, sorted(values)
    assert parse(SELF_TEST_EXOTIC_VALUES)[
        ("logit.shape.attributes", "metric", "syslog_in", "tap_input")
    ].samples == [6.0]

    # A truncated container is a parse error, not a silently short attribute set: a half-written
    # final line (a capture cut off mid-flush) must not read as a valid, narrower event.
    for broken in ('a=[1, 2 b="x"', "a={k=1 b=2", "a=<12 bytes", "a="):
        try:
            parse_attrs(broken)
        except ValueError:
            continue
        raise AssertionError(f"a malformed attrs line must raise, got a parse of {broken!r}")


# ---- entry point ---------------------------------------------------------------------------------


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--shape-log", help="the file_out capture to parse")
    ap.add_argument("--out-dir", help="where summary.json / summary.md are written")
    ap.add_argument(
        "--provenance",
        help="the run's provenance.txt, whose `representativeness:` line becomes the banner",
    )
    ap.add_argument(
        "--source-labels",
        help='optional JSON: {"<source>": {"tier": ..., "format": ...}} -- where each source\'s'
        " own log/metric format came from",
    )
    ap.add_argument(
        "--append",
        help="a producer-written markdown file appended to the end of summary.md (and recorded in"
        " summary.json as `producer_section`) -- the reading of these numbers only that producer"
        " can give, kept out of this general engine",
    )
    ap.add_argument("--self-test", action="store_true", help="check the parser against a real render")
    args = ap.parse_args()

    if args.self_test:
        self_test()
        return
    if not args.shape_log or not args.out_dir:
        ap.error("--shape-log and --out-dir are required unless --self-test is given")

    text = pathlib.Path(args.shape_log).read_text()
    series = parse(text)
    if not series:
        print(f"summarize: no logit.shape.* records in {args.shape_log}", file=sys.stderr)
        sys.exit(1)
    summary = summarize(series)
    summary.update(read_provenance(pathlib.Path(args.provenance) if args.provenance else None))
    if args.source_labels:
        summary["source_labels"] = json.loads(pathlib.Path(args.source_labels).read_text())

    if args.append:
        summary["producer_section"] = pathlib.Path(args.append).read_text()

    out_dir = pathlib.Path(args.out_dir)
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=False) + "\n")
    markdown = render_markdown(summary)
    (out_dir / "summary.md").write_text(markdown)
    print(markdown)


if __name__ == "__main__":
    main()
