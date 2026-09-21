#!/usr/bin/env python3
"""The shape survey's acceptance test: does `shape` reproduce numbers somebody else already counted?

`testdata/interop/statsd/` is 56 real UDP datagrams from two real statsd clients, and how they are
packed -- lines per datagram, tags per line, tagged versus tagless -- is independently known
(`testdata/interop/statsd/README.md`). So the instrument this survey is built on can be checked
rather than trusted: replay that corpus at `statsd_in`, let `shape` measure it, and assert the
measurements equal what a completely separate parser derives from the same bytes.

**This script derives its expectations from the `.raw` files, not from the README** -- a number
copied out of a document would only prove the document and this file agree. It also parses
`shape.log` with its own reader rather than importing `summarize.py`'s: if the two shared a parser,
a parser bug would cancel itself out on both sides of the comparison, which is exactly the failure
this check exists to catch.

The mapping from wire to measurement is **not 1:1**, and the difference is the interesting part:

  logit.shape.batch.events  <-> events per datagram. Not *lines* per datagram: `statsd_in` emits
                                one event per value for a `c`/`g` line (`v1:v2|c` is two counter
                                points) and one event per line for `ms`/`h`/`d`/`s` (every value on
                                the line shares one `Samples`/`SetMembers` record). With
                                `receive.batch_max_events: 1` the accumulator never splits or
                                merges a datagram's decode output, so one datagram is one batch.

  logit.shape.attributes    <-> tags per line, plus the `statsd.*` carriers the decoder stamps:
  (at tap_input)                `statsd.container_id` for a `|c:<id>` segment (the DogStatsD client
                                emits one unprompted, having detected its own container),
                                `statsd.timestamp` for a `|T<secs>` segment, and `statsd.type` on
                                `ms`/`h`/`d` lines only -- the wire type letter, which those three
                                share one `Samples` shape for. Repeated tag *keys* merge into one
                                attribute holding an array, so a tag count is distinct keys, not
                                tokens.

Both comparisons are over **multisets**: the survey never asks in what order datagrams arrived, and
UDP would not promise one anyway.

Stdlib only (`python:3.12-slim`, no pip). Exits non-zero, loudly and specifically, on any mismatch.

Usage:
    check_interop.py --corpus /corpus/statsd --shape-log /out/shape.log
    check_interop.py --self-test
"""

import argparse
import glob
import pathlib
import sys
from collections import Counter

#: The `source` tag `shape` stamps for the statsd listener (the component's name in
#: tools/shape-survey/configs/interop.yaml) and the tap whose measurements are the wire's own.
SOURCE = "statsd_in"
TAP = "tap_input"


# ---- deriving the expectation from the raw datagrams ---------------------------------------------


def parse_line(line: str) -> tuple[int, int] | None:
    """One statsd line -> (events it decodes to, attributes each of those events carries).

    A deliberately tiny reimplementation of the grammar `crates/logit-inputs/src/statsd.rs` reads:
    `<name>:<v1>[:<v2>...]|<type>[|@rate][|#tags][|c:<id>][|T<secs>]`. It knows nothing about
    logit's decoder beyond what the wire says, which is the point.

    Returns None for a line this check does not model -- DogStatsD events (`_e{`) and service
    checks (`_sc|`), which the corpus does not contain (its README says so, and `--self-test`
    pins that this function would refuse them rather than guess).
    """
    if line.startswith("_e{") or line.startswith("_sc|"):
        return None
    name, _, rest = line.partition(":")
    if not name or not rest:
        return None
    parts = rest.split("|")
    values, type_part, extras = parts[0], parts[1], parts[2:]

    tag_keys: set[str] = set()
    carriers = 0
    for extra in extras:
        if extra.startswith("#"):
            for token in extra[1:].split(","):
                if token:
                    tag_keys.add(token.split(":", 1)[0])
        elif extra.startswith("c:"):
            carriers += 1  # statsd.container_id
        elif extra.startswith("T"):
            carriers += 1  # statsd.timestamp

    if type_part in ("c", "g"):
        events = len(values.split(":"))
        attributes = len(tag_keys) + carriers
    elif type_part in ("ms", "h", "d"):
        events = 1
        attributes = len(tag_keys) + carriers + 1  # + statsd.type
    elif type_part == "s":
        events = 1
        attributes = len(tag_keys) + carriers
    else:
        return None
    return events, attributes


def derive(corpus: pathlib.Path) -> dict:
    """Everything this check expects, derived from the `.raw` datagrams alone."""
    batch_events: Counter[int] = Counter()
    attributes: Counter[int] = Counter()
    tags_per_line: Counter[int] = Counter()
    lines_per_datagram: Counter[int] = Counter()
    datagram_bytes: list[int] = []
    unmodelled: list[str] = []

    for path in sorted(glob.glob(str(corpus / "*.raw"))):
        data = pathlib.Path(path).read_bytes()
        datagram_bytes.append(len(data))
        lines = [line for line in data.decode("utf-8").split("\n") if line]
        lines_per_datagram[len(lines)] += 1
        events_here = 0
        for line in lines:
            parsed = parse_line(line)
            if parsed is None:
                unmodelled.append(line)
                continue
            events, attrs = parsed
            events_here += events
            attributes[attrs] += events
            # Tags-per-line, the figure testdata/interop/statsd/README.md's table quotes, kept
            # separately from the attribute count so the two can be reported side by side: the
            # difference between them is exactly the `statsd.*` carriers.
            tag_part = next((p[1:] for p in line.split("|")[2:] if p.startswith("#")), "")
            tags_per_line[len({t.split(":", 1)[0] for t in tag_part.split(",") if t})] += 1
        batch_events[events_here] += 1

    return {
        "batch_events": batch_events,
        "attributes": attributes,
        "tags_per_line": tags_per_line,
        "lines_per_datagram": lines_per_datagram,
        "datagrams": len(datagram_bytes),
        "bytes": sum(datagram_bytes),
        "unmodelled": unmodelled,
    }


# ---- reading back what `shape` reported ----------------------------------------------------------


def read_reported(shape_log: pathlib.Path) -> dict[str, Counter]:
    """`logit.shape.batch.events` and `logit.shape.attributes` for the statsd tap, as multisets.

    Its own reader, deliberately -- see this module's docstring. It needs only two facts out of
    the render: which block an `attrs` line puts us in, and the `samples=[...]` on a `metric`
    line, so the whole grammar it implements is those two shapes.
    """
    reported = {"batch_events": Counter(), "attributes": Counter()}
    in_scope = False

    for raw in shape_log.read_text().splitlines():
        if raw and not raw.startswith(" "):
            in_scope = False
            continue
        line = raw.strip()
        if line.startswith("attrs "):
            in_scope = f'source="{SOURCE}"' in line and f'tap="{TAP}"' in line
            continue
        if not in_scope or not line.startswith("metric "):
            continue
        body = line[len("metric ") :].strip()
        name, _, rendered = body.partition(" ")
        target = {
            "logit.shape.batch.events": "batch_events",
            "logit.shape.attributes": "attributes",
        }.get(name)
        if target is None or not rendered.startswith("samples=["):
            continue
        values = rendered[len("samples=[") : rendered.index("]")]
        for value in values.split(","):
            if value:
                reported[target][int(float(value))] += 1
    return reported


# ---- comparison ----------------------------------------------------------------------------------


def histogram(counts: Counter) -> str:
    total = sum(counts.values())
    body = ", ".join(f"{k}x{v}" for k, v in sorted(counts.items()))
    return f"n={total} [{body}]"


def compare(label: str, expected: Counter, reported: Counter) -> bool:
    if expected == reported:
        print(f"  OK   {label}: {histogram(expected)}")
        return True
    print(f"  FAIL {label}")
    print(f"       derived from the .raw files: {histogram(expected)}")
    print(f"       reported by shape:           {histogram(reported)}")
    only_expected = expected - reported
    only_reported = reported - expected
    if only_expected:
        print(f"       missing from shape's output: {histogram(only_expected)}")
    if only_reported:
        print(f"       extra in shape's output:     {histogram(only_reported)}")
    return False


def check(corpus: pathlib.Path, shape_log: pathlib.Path) -> int:
    expected = derive(corpus)
    reported = read_reported(shape_log)

    print(f"check_interop: {expected['datagrams']} datagram(s), {expected['bytes']} bytes in {corpus}")
    print(f"  lines per datagram (derived):  {histogram(expected['lines_per_datagram'])}")
    print(f"  tags per line      (derived):  {histogram(expected['tags_per_line'])}")
    print("  (tags per line is the wire figure testdata/interop/statsd/README.md tabulates;")
    print("   the attribute counts below add statsd_in's own statsd.* carriers -- see the docstring)")

    if expected["unmodelled"]:
        print(
            "check_interop: these lines are outside this checker's grammar, so the comparison"
            " below would be incomplete:\n  " + "\n  ".join(expected["unmodelled"][:10]),
            file=sys.stderr,
        )
        return 1
    if not reported["batch_events"] or not reported["attributes"]:
        print(
            f"check_interop: no logit.shape.* samples for source={SOURCE} tap={TAP} in"
            f" {shape_log} -- the replay never reached the listener, or the tap is misnamed",
            file=sys.stderr,
        )
        return 1

    ok = compare(
        "logit.shape.batch.events <-> events per datagram",
        expected["batch_events"],
        reported["batch_events"],
    )
    ok &= compare(
        "logit.shape.attributes <-> tags + statsd.* carriers per event",
        expected["attributes"],
        reported["attributes"],
    )

    if not ok:
        print(
            "\ncheck_interop: `shape` did not reproduce the corpus's own numbers. Do NOT adjust"
            " this check to pass -- either the instrument, the decoder, or this derivation is"
            " wrong, and which one it is has to be established before any survey result built on"
            " `shape` means anything.",
            file=sys.stderr,
        )
        return 1
    print("check_interop: shape reproduced the recorded corpus exactly")
    return 0


# ---- self-test -----------------------------------------------------------------------------------


def self_test() -> None:
    # A tagged DogStatsD counter with a container id: 6 tag keys + statsd.container_id = 7.
    assert parse_line(
        "app.http.requests.count:1|c|#env:prod,svc:api,endpoint:/,method:GET,status:200,"
        "region:eu|c:in-deadbeef"
    ) == (1, 7)
    # The same line with no tags and no carriers at all: the plain-statsd shape.
    assert parse_line("app.http.requests.count:1|c") == (1, 0)
    # A timer carries statsd.type on top of its tags.
    assert parse_line("app.db.query.duration_ms:12.5|ms|#env:prod") == (1, 2)
    # Several values on one timer line stay ONE event (one Samples record); on a counter line they
    # are one event each.
    assert parse_line("app.db.query.duration_ms:1:2:3|ms") == (1, 1)
    assert parse_line("app.cache.lookups.count:1:2:3|c") == (3, 0)
    # A repeated tag key merges into one attribute, so it counts once.
    assert parse_line("app.x:1|c|#k:a,k:b") == (1, 1)
    # A sample rate is not an attribute.
    assert parse_line("app.x:1|c|@0.1") == (1, 0)
    # A set line carries no statsd.type.
    assert parse_line("app.users.active:u1:u2|s|#env:prod") == (1, 1)
    # Events and service checks are refused rather than guessed at.
    assert parse_line("_e{5,4}:title|text") is None
    assert parse_line("_sc|svc|0") is None

    render = (
        "2026-09-20T00:00:00.000000000Z\n"
        '  attrs   signal="metric" source="statsd_in" tap="tap_input"\n'
        "  metric  logit.shape.attributes samples=[7,7,0] rate=1\n"
        "\n"
        "2026-09-20T00:00:00.000000000Z\n"
        '  attrs   source="statsd_in" tap="tap_input"\n'
        "  metric  logit.shape.batch.events samples=[1,11] rate=1\n"
        "\n"
        "2026-09-20T00:00:00.000000000Z\n"
        '  attrs   signal="metric" source="syslog_udp_in" tap="tap_input"\n'
        "  metric  logit.shape.attributes samples=[4] rate=1\n"
    )
    tmp = pathlib.Path("/tmp/check_interop_self_test.log")
    tmp.write_text(render)
    reported = read_reported(tmp)
    assert reported["attributes"] == Counter({7: 2, 0: 1}), reported["attributes"]
    assert reported["batch_events"] == Counter({1: 1, 11: 1}), reported["batch_events"]
    tmp.unlink()

    assert compare("equal", Counter({1: 2}), Counter({1: 2}))
    assert not compare("unequal", Counter({1: 2}), Counter({1: 1}))
    print("check_interop: self-test passed")


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--corpus", help="testdata/interop/statsd, mounted read-only")
    ap.add_argument("--shape-log", help="the run's shape.log")
    ap.add_argument("--self-test", action="store_true")
    args = ap.parse_args()

    if args.self_test:
        self_test()
        return
    if not args.corpus or not args.shape_log:
        ap.error("--corpus and --shape-log are required unless --self-test is given")
    sys.exit(check(pathlib.Path(args.corpus), pathlib.Path(args.shape_log)))


if __name__ == "__main__":
    main()
