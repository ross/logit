#!/usr/bin/env python3
"""Fold a `shape.log` capture into `summary.json` + `summary.md`, for `script/shape-survey`.

Reads `file_out`'s `format: json` output (NDJSON, one object per event), which carries raw metric
values rather than re-sketched quantiles. `iter_events` is the one reader; `oteldemo.sh`'s
`resource: keep` section imports `iter_samples` from it.

What it does with each `logit.shape.*` record, per series (a series being the metric name plus the
`signal`/`source`/`tap` tags `shape` stamps):

  kind samples   `values` concatenated across every flush window: raw observations, one per
                 event (or per batch), so the summary covers the whole population
  kind sum       `value` summed (a counter, delta temporality from `aggregate`)
  kind gauge     `value` of the last record wins (cumulative-since-start gauges: distinct keys,
                 key-set shares)

A sketched `logit.shape.*` series is a hard failure. Past `max_samples_per_series`, `aggregate`
falls back to a `DdSketch` (`kind: distribution`), which would make every distribution
silently approximate. This exits non-zero and names the series, so the config's cap gets raised.

Percentiles are nearest-rank. A p99 prints `n/a` below 100 values and a p90 below 10, and every
table carries its own value count.

Every summary opens with the representativeness banner from `provenance.txt` (README
"Representativeness is structural"). `--source-labels` adds a per-source table of where each
source's format came from. Stdlib only.

Usage:
    summarize.py --shape-log /out/shape.log --out-dir /out --provenance /out/provenance.txt
    summarize.py --self-test        # parse embedded NDJSON captures, check the numbers
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


def iter_events(text: str):
    """Yields each NDJSON line of a `format: json` capture as a dict.

    A line that is not a JSON object, an object without `timestamp`, or a `metrics` item without a
    string `name` and `kind` raises `ValueError`: a half-written final line (a capture cut off
    mid-flush) must not read as a valid, narrower event. Blank lines are skipped.
    """
    for number, raw in enumerate(text.splitlines(), 1):
        if not raw.strip():
            continue
        try:
            obj = json.loads(raw)
        except json.JSONDecodeError as err:
            raise ValueError(f"line {number} is not valid JSON: {err}") from err
        if not isinstance(obj, dict) or "timestamp" not in obj:
            raise ValueError(f"line {number} is not an event object with a timestamp: {raw[:80]!r}")
        for item in obj.get("metrics", []):
            if (
                not isinstance(item, dict)
                or not isinstance(item.get("name"), str)
                or not isinstance(item.get("kind"), str)
            ):
                raise ValueError(f"line {number} has a metrics item without name and kind: {item!r}")
        yield obj


def iter_samples(text: str):
    """Yields `(attributes, resource_attributes, [(name, values)])` per event.

    `values` is the `values` list of each `kind: samples` metrics item; other kinds are omitted.
    """
    for obj in iter_events(text):
        samples = [
            (item["name"], item.get("values", []))
            for item in obj.get("metrics", [])
            if item["kind"] == "samples"
        ]
        yield obj.get("attributes", {}), obj.get("resource", {}).get("attributes", {}), samples


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

    `attributes` carries an event's tags (`signal`/`source`/`tap`, read from the top level only);
    every `logit.shape.*` metrics item on the event belongs to them.
    """
    series: OrderedDict[tuple, Series] = OrderedDict()
    sketched: list[str] = []

    for obj in iter_events(text):
        attrs = obj.get("attributes", {})
        tags = {k: str(attrs[k]) for k in TAG_KEYS if k in attrs}
        for item in obj.get("metrics", []):
            name, kind = item["name"], item["kind"]
            if not name.startswith("logit.shape."):
                # Skipped, not an error: a producer may point other traffic at the same file_out.
                continue
            entry = series.setdefault((name, *(tags.get(k, "") for k in TAG_KEYS)), Series(name, tags))

            if kind == "samples":
                entry.samples.extend(float(v) for v in item.get("values", []))
            elif kind == "sum":
                entry.total = (entry.total or 0.0) + float(item["value"])
            elif kind == "gauge":
                entry.gauge = float(item["value"])
            elif kind == "distribution":
                sketched.append(f"{name}{sorted(tags.items())}")
            else:
                raise ValueError(f"unrecognized metric kind {kind!r} on {name!r}")

    if sketched:
        raise SystemExit(
            "summarize: a logit.shape.* series was written as a DdSketch, not raw samples:\n  "
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

#: An NDJSON excerpt shaped like a real `shape.log` from the interop producer (`file_out`,
#: `format: json`). `script/shape-survey` runs `--self-test` before every survey.
SELF_TEST_RENDER = "\n".join(
    [
        '{"timestamp":"2026-09-20T15:56:20.153724900Z","metrics":['
        '{"name":"logit.shape.events","kind":"sum","value":3,"temporality":"delta","monotonic":true},'
        '{"name":"logit.shape.attributes","kind":"samples","values":[7,7,0],"sample_rate":1},'
        '{"name":"logit.shape.value_depth","kind":"samples","values":[0,0,0],"sample_rate":1},'
        '{"name":"logit.shape.key_bytes","kind":"samples","values":[3,11,4,8,6,20,9],"sample_rate":1},'
        '{"name":"logit.shape.values.string","kind":"sum","value":14,"temporality":"delta","monotonic":true}],'
        '"attributes":{"signal":"metric","source":"statsd_in","tap":"tap_input"}}',
        '{"timestamp":"2026-09-20T15:56:20.153724900Z","metrics":['
        '{"name":"logit.shape.batch.events","kind":"samples","values":[1,1,2],"sample_rate":1}],'
        '"attributes":{"source":"statsd_in","tap":"tap_input"}}',
        '{"timestamp":"2026-09-20T15:56:20.153724900Z","metrics":['
        '{"name":"logit.shape.distinct_keys","kind":"gauge","value":9},'
        '{"name":"logit.shape.keyset_share.top1","kind":"gauge","value":0.5},'
        '{"name":"logit.shape.tracking_overflow","kind":"gauge","value":0}],'
        '"attributes":{"tap":"tap_input"}}',
    ]
)


#: A second excerpt shaped like an `oteldemo` `resource: keep` capture, where the batch's resource
#: attributes appear under `resource.attributes`. The structure is the capture's; the values are
#: neutral placeholders, because a fixture quoting a capture must not carry the identity `shape`'s
#: output never does.
#:
#: It holds what the reader has to survive: an array of strings holding commas, spaces, and an
#: `=` (`process.command_args`, on the resource of every OTel SDK that detects a process), a bare
#: integer beside it, empty strings, and a string carrying spaces, `=`, and `:`.
SELF_TEST_RESOURCE_KEEP = "\n".join(
    [
        '{"timestamp":"2026-09-20T20:43:57.818255215Z","metrics":['
        '{"name":"logit.shape.attributes","kind":"samples","values":[11,9],"sample_rate":1},'
        '{"name":"logit.shape.events","kind":"sum","value":2,"temporality":"delta","monotonic":true}],'
        '"attributes":{"signal":"log","source":"otlp_gateway","tap":"tap_input"},'
        '"resource":{"attributes":{"process.pid":1,"process.executable.path":"/opt/app/bin/node",'
        '"process.command_args":["/opt/app/bin/node","--require=./Instrumentation.js","/app/server.js"],'
        '"process.command_line":"/opt/jdk/bin/java -javaagent:/app/agent.jar -Xmx200m example.Service",'
        '"host.name":"host-placeholder",'
        '"container.id":"0000000000000000000000000000000000000000000000000000000000000000",'
        '"service.name":"frontend",'
        '"os.description":"Linux host-placeholder 0.0.0-0 #1 SMP PLACEHOLDER x86_64",'
        '"host.cpu.cache.l2.size":1024,"zone_name":"","cluster_name":""}}}',
        '{"timestamp":"2026-09-20T20:43:57.818255215Z","metrics":['
        '{"name":"logit.shape.attributes","kind":"samples","values":[7],"sample_rate":1}],'
        '"attributes":{"signal":"log","source":"otlp_gateway","tap":"tap_input"},'
        '"resource":{"attributes":{"process.command_args":["./shipping"],"service.name":"shipping"}}}',
    ]
)

#: The remaining `Value` variants that can sit in `attributes`: a nested array, a map (a
#: `syslog_in` structured-data element) including a nested one, an array of objects, a
#: `Value::Bytes` (`b"..."` text with a space in it), a key with a space, a string value carrying
#: `[`, `]`, `{`, `}`, `,`, `=`, a space, and an escaped quote, a non-finite float (`"NaN"`), and
#: empty `{}`/`[]`. `nested_tags` holds `signal`, `source`, and `tap` keys inside another attribute:
#: they are not tags. Synthetic: a reader test, not a measurement.
SELF_TEST_EXOTIC_VALUES = (
    '{"timestamp":"2026-09-20T20:43:57.818255215Z","metrics":['
    '{"name":"logit.shape.attributes","kind":"samples","values":[6],"sample_rate":1}],'
    '"attributes":{"signal":"metric","nested":[1,[2,3],{"a":1,"b":[4,5]}],'
    '"sd":{"origin":{"ip":"10.0.0.1","port":514},"note":"a=b, c=d"},'
    '"items":[{"name":"x"},{"name":"y","tags":["t"]}],"blob":"b\\"\\\\xff\\\\x00 raw\\"",'
    '"odd key":"[{x=1}], \\"quoted\\"","ratio":"NaN","empty_map":{},"empty_array":[],'
    '"nested_tags":{"signal":"other","source":"other","tap":"other"},'
    '"source":"syslog_in","tap":"tap_input","trailing":true}}'
)


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
        '{"timestamp":"2026-09-20T15:56:20.153724900Z","metrics":['
        '{"name":"logit.shape.attributes","kind":"distribution","count":4096,"sum":1,"p50":6}],'
        '"attributes":{"signal":"metric","source":"statsd_in","tap":"tap_input"}}'
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
    """The NDJSON reader, over every `Value` variant that can appear in `attributes`.

    A `resource: keep` capture puts the batch's resource attributes (arrays holding `=`, such as
    `process.command_args`) beside the tags; the tags come from `attributes` alone, so nothing in a
    value or in the resource can be mistaken for one.
    """
    keep = parse(SELF_TEST_RESOURCE_KEEP)

    events = list(iter_samples(SELF_TEST_RESOURCE_KEEP))
    attrs, resource, samples = events[0]
    assert resource["process.command_args"] == [
        "/opt/app/bin/node",
        "--require=./Instrumentation.js",
        "/app/server.js",
    ], resource["process.command_args"]
    assert resource["process.pid"] == 1, resource["process.pid"]
    assert resource["service.name"] == "frontend", resource["service.name"]
    assert resource["zone_name"] == "" and resource["cluster_name"] == "", resource
    assert resource["process.command_line"].startswith("/opt/jdk/bin/java -javaagent:"), resource
    assert resource["host.cpu.cache.l2.size"] == 1024, resource
    assert (attrs["signal"], attrs["source"], attrs["tap"]) == ("log", "otlp_gateway", "tap_input")
    assert samples == [("logit.shape.attributes", [11, 9])], samples
    assert events[1][1] == {"process.command_args": ["./shipping"], "service.name": "shipping"}
    assert list(iter_samples(SELF_TEST_RENDER))[0][1] == {}, "no resource on a resource: drop capture"

    # Two blocks, same series key (the resource is not part of it, by design), so the samples
    # concatenate as they do for a `resource: drop` capture.
    kept = keep[("logit.shape.attributes", "log", "otlp_gateway", "tap_input")]
    assert kept.samples == [11.0, 9.0, 7.0], kept.samples
    assert keep[("logit.shape.events", "log", "otlp_gateway", "tap_input")].total == 2.0
    assert summarize(keep)["attribute_widths"], "a resource: keep capture must still summarize"

    # Nested containers, a map, bytes text, a key with a space, and a string value full of the
    # delimiters come back as the values the encoder wrote.
    values = next(iter_events(SELF_TEST_EXOTIC_VALUES))["attributes"]
    assert values["nested"] == [1, [2, 3], {"a": 1, "b": [4, 5]}], values["nested"]
    assert values["sd"] == {"origin": {"ip": "10.0.0.1", "port": 514}, "note": "a=b, c=d"}, values["sd"]
    assert values["items"] == [{"name": "x"}, {"name": "y", "tags": ["t"]}], values["items"]
    assert values["blob"] == 'b"\\xff\\x00 raw"', values["blob"]
    assert values["odd key"] == '[{x=1}], "quoted"', values["odd key"]
    assert values["ratio"] == "NaN" and float(values["ratio"]) != float(values["ratio"])
    assert values["empty_map"] == {} and values["empty_array"] == [], values
    assert values["trailing"] is True, values["trailing"]
    exotic = parse(SELF_TEST_EXOTIC_VALUES)
    assert exotic[("logit.shape.attributes", "metric", "syslog_in", "tap_input")].samples == [6.0]
    assert len(exotic) == 1, "a `signal` nested in another attribute is not a tag"

    good = SELF_TEST_RESOURCE_KEEP.splitlines()[1]
    for broken in (
        good[: len(good) // 2],  # a capture cut off mid-line
        '{"metrics":[]}',  # no timestamp
        '{"timestamp":"2026-09-20T20:43:57Z","metrics":[{"kind":"sum","value":1}]}',  # no name
        '{"timestamp":"2026-09-20T20:43:57Z","metrics":[{"name":"logit.shape.x","kind":"bogus"}]}',
        "[1, 2]",  # not an object
    ):
        try:
            parse(broken)
        except ValueError:
            continue
        raise AssertionError(f"a malformed line must raise, got a parse of {broken!r}")


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
    ap.add_argument("--self-test", action="store_true", help="check the reader against embedded NDJSON captures")
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
