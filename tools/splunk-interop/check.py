#!/usr/bin/env python3
"""Confirms each leg of `script/splunk-interop` against Splunk, then probes Splunk directly.

Runs in a throwaway `python:3.12-slim` container on the stack's network, stdlib only, after the
harness has held the stack up for its window and copied every service's log into the run
directory. Mounts:

    /out  the run directory: logs/<service>.log, replay.log, ack-telemetry.log, tcpout/;
          results.md and results.json land here

Searches go through Splunk's REST API on :8089 (`/services/search/jobs/export`, `output_mode=json`)
as admin; the probes post to HEC on :8088. A leg is PASS (Splunk holds what the leg sent, in the
shape docs/plans/splunk-relay.md expects), GAP (it arrived, with a difference the README records),
or FAIL. A probe row is INFO: what Splunk answered, recorded for the plan. Exits 1 on any FAIL.
"""

import base64
import gzip
import json
import pathlib
import re
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
import zlib

HEC = "http://splunk:8088"
API = "https://splunk:8089"
TOKEN = "11111111-1111-1111-1111-111111111111"
ACK_TOKEN = "22222222-2222-2222-2222-222222222222"
TCPOUT_TOKEN = "44444444-4444-4444-4444-444444444444"
ADMIN = "Basic " + base64.b64encode(b"admin:logit-splunk-interop").decode()
# Splunk's management port serves a self-signed certificate.
INSECURE = ssl._create_unverified_context()

OUT = pathlib.Path("/out")
RUN = f"r{int(time.time())}"


# ---- HTTP ---------------------------------------------------------------------------------------


def request(method, url, body=None, headers=None):
    """(status, headers, body text); an HTTP error status is a result, not an exception."""
    req = urllib.request.Request(url, data=body, method=method, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=30, context=INSECURE) as response:
            return response.status, dict(response.headers), response.read().decode(errors="replace")
    except urllib.error.HTTPError as err:
        return err.code, dict(err.headers), err.read().decode(errors="replace")


def hec(path, body, token=TOKEN, headers=None):
    all_headers = {"Authorization": f"Splunk {token}", **(headers or {})}
    status, _, text = request("POST", HEC + path, body, all_headers)
    return status, text


def objects(*objs):
    return "".join(json.dumps(o) for o in objs).encode()


def search(query):
    """Result rows of one export search over all time."""
    body = urllib.parse.urlencode({"search": query, "output_mode": "json", "earliest_time": "0"})
    status, _, text = request("POST", API + "/services/search/jobs/export", body.encode(),
                              {"Authorization": ADMIN})
    if status != 200:
        raise RuntimeError(f"search {query!r}: {status} {text[:200]}")
    rows = []
    for line in text.splitlines():
        if line.strip():
            item = json.loads(line)
            if "result" in item:
                rows.append(item["result"])
    return rows


def search_until(query, enough, timeout=60):
    """Polls `query` until `enough(rows)` or `timeout`: indexing trails a HEC `200`."""
    deadline = time.monotonic() + timeout
    while True:
        rows = search(query)
        if enough(rows) or time.monotonic() > deadline:
            return rows
        time.sleep(3)


def rest_json(path):
    status, _, text = request("GET", f"{API}{path}?output_mode=json", None, {"Authorization": ADMIN})
    return json.loads(text) if status == 200 else {}


def as_list(value):
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


# ---- logs ---------------------------------------------------------------------------------------


def log(name):
    path = OUT / "logs" / f"{name}.log"
    return path.read_text(errors="replace") if path.exists() else ""


def strip_ansi(text):
    return re.sub(r"\x1b\[[0-9;]*m", "", text)


def rendered_events(path):
    """`stdio_out`'s human render, as (attrs line, metric line) pairs."""
    if not path.exists():
        return []
    events, attrs = [], ""
    for line in path.read_text(errors="replace").splitlines():
        stripped = line.strip()
        if stripped.startswith("attrs "):
            attrs = stripped
        elif stripped.startswith("metric "):
            events.append((attrs, stripped))
    return events


def telemetry_sum(events, metric, **attrs):
    total = 0.0
    for attr_line, metric_line in events:
        if metric_line.startswith(f"metric  {metric} ") and all(f'{k}="{v}"' in attr_line for k, v in attrs.items()):
            match = re.search(r"sum=([0-9.e+-]+)", metric_line)
            if match:
                total += float(match.group(1))
    return int(total)


def rejections(service):
    """The sink's warn-level diagnostics about a refused request."""
    lines = strip_ansi(log(service)).splitlines()
    return [line for line in lines if re.search(r"request_rejected|token_rejected|send_failed", line)]


# ---- legs ---------------------------------------------------------------------------------------


def leg_logs():
    rows = search_until('search index=main sourcetype="logit:test" | head 1 | table _raw host source'
                        ' otel.log.severity.text otel.log.severity.number trace_id span_id detail.stage detail.ok leg',
                        lambda r: len(r) >= 1)
    if not rows:
        return "FAIL", "no sourcetype=logit:test events in main"
    row = rows[0]
    fields = {k: row.get(k) for k in ("host", "source", "otel.log.severity.text", "otel.log.severity.number",
                                      "trace_id", "span_id", "detail.stage", "detail.ok", "leg")}
    indexed = search('| tstats count where index=main sourcetype="logit:test" by detail.stage, otel.log.severity.number')
    problems = [k for k, v in fields.items() if v in (None, "")]
    if rejections("logit-hec-logs"):
        problems.append(f"sink logged {len(rejections('logit-hec-logs'))} rejection(s)")
    if not indexed:
        problems.append("tstats finds no indexed detail.stage")
    detail = f"_raw={row.get('_raw')!r}; fields {fields}; tstats by indexed fields: {indexed[:1]}"
    return ("FAIL" if problems else "PASS"), "; ".join(problems + [detail])


def leg_metrics():
    rows = search_until('| mstats latest(_value) as v where index=metrics metric_name="splunk_interop.*" by metric_name',
                        lambda r: any(x["metric_name"].startswith("splunk_interop.distribution") for x in r), 90)
    names = sorted(r["metric_name"] for r in rows)
    want = [
        "splunk_interop.gauge", "splunk_interop.sum_cumulative", "splunk_interop.sum_delta",
        "splunk_interop.samples_count", "splunk_interop.samples_sum", "splunk_interop.samples_min",
        "splunk_interop.samples_max", "splunk_interop.set_members", "splunk_interop.histogram_sum",
        "splunk_interop.histogram_count", "splunk_interop.histogram_bucket", "splunk_interop.summary_sum",
        "splunk_interop.summary_count", "splunk_interop.distribution_count", "splunk_interop.distribution_sum",
        "splunk_interop.distribution_p50", "splunk_interop.set",
    ]
    missing = [n for n in want if n not in names]
    problems = [f"missing {missing}"] if missing else []
    if any("exphist" in n for n in names):
        problems.append("an exponential histogram reached Splunk")
    types = search('| mcatalog values(metric_type) as t where index=metrics metric_name="splunk_interop.*"')
    by_type = search('| mstats count(_value) as n where index=metrics metric_name="splunk_interop.*" by metric_type')
    buckets = search('| mstats max(_value) as c where index=metrics metric_name="splunk_interop.histogram_bucket" by le')
    perc = search('| mstats max(_value) as c where index=metrics metric_name="splunk_interop.histogram_bucket" by le'
                  ' | `histperc(0.5, c, le)`')
    if rejections("logit-hec-metrics"):
        problems.append(f"sink logged {len(rejections('logit-hec-metrics'))} rejection(s)")
    detail = (f"{len(names)} series: {names}; metric_type values {types[:1]}; count by metric_type "
              f"{[(r.get('metric_type'), r.get('n')) for r in by_type]}; histogram buckets "
              f"{[(r.get('le'), r.get('c')) for r in buckets]}; histperc(0.5) {perc[:1]}")
    return ("FAIL" if problems else "PASS"), "; ".join(problems + [detail])


def leg_spans():
    rows = search_until('search index=main sourcetype="logit:span" | head 1 | spath | table *', lambda r: len(r) >= 1)
    if not rows:
        return "FAIL", "no sourcetype=logit:span events in main"
    # Splunk extracts the JSON `event` itself and `spath` extracts it again: one value each.
    row = {k: (v[0] if isinstance(v, list) else v) for k, v in rows[0].items()}
    wanted = {k: row.get(k) for k in ("trace_id", "span_id", "name", "kind", "status.code", "status.message",
                                      "start_time", "end_time", "events{}.name", "service.name")}
    problems = [k for k, v in wanted.items() if v in (None, "")]
    if row.get("kind") != "SPAN_KIND_SERVER" or row.get("status.code") != "STATUS_CODE_ERROR":
        problems.append("kind or status.code not the exporter's enum name")
    return ("FAIL" if problems else "PASS"), "; ".join(problems + [f"spath {wanted}"])


def leg_ack():
    telemetry = rendered_events(OUT / "ack-telemetry.log")
    acked = telemetry_sum(telemetry, "logit.output.acks", result="acked")
    timeout = telemetry_sum(telemetry, "logit.output.acks", result="timeout")
    unsupported = telemetry_sum(telemetry, "logit.output.acks", result="unsupported")
    polls = telemetry_sum(telemetry, "logit.output.requests", route="ack")
    rows = search('search index=main sourcetype="logit:ack" | stats count')
    count = int(rows[0]["count"]) if rows else 0
    detail = (f"acks acked={acked} timeout={timeout} unsupported={unsupported}; {polls} /ack polls; "
              f"{count} events indexed")
    if acked >= 1 and timeout == 0 and unsupported == 0 and count >= 1:
        return "PASS", detail
    return "FAIL", detail


#: What the relay leg must put in Splunk from each recorded producer: (label, search, minimum).
RELAY_EXPECTED = [
    ("docker driver", 'search index=main host="splunk-docker-fixture" | stats count', 6),
    ("java appender", 'search index=main source="java:logback" | stats count', 9),
    ("sc4s lines", 'search index=osnix sourcetype="nix:syslog" host="sc4s-fixture-host" | stats count', 3),
    ("sc4s own events", 'search index=main sourcetype="sc4s:*" | stats count', 4),
    ("otel logs", 'search index=main sourcetype="otel:logs" | stats count', 3),
    ("otel raw", 'search index=main "the message" NOT sourcetype="otel:logs" | stats count', 3),
    ("otel spans", 'search index=main sourcetype="otel:traces" | stats count', 6),
]


def leg_relay():
    replay = (OUT / "replay.log").read_text().splitlines() if (OUT / "replay.log").exists() else []
    bad = [line for line in replay if not line.startswith("2")]
    problems = [] if replay else ["replay.log is missing"]
    if bad:
        problems.append(f"non-2xx replies: {bad}")
    counts = {}
    for label, query, least in RELAY_EXPECTED:
        rows = search_until(query, lambda r, least=least: bool(r) and int(r[0]["count"]) >= least)
        counts[label] = int(rows[0]["count"]) if rows else 0
        if counts[label] < least:
            problems.append(f"{label}: {counts[label]} < {least}")
    metrics = search('| mstats latest(_value) where index=metrics metric_name="gen*" by metric_name')
    names = sorted(r["metric_name"] for r in metrics)
    if not {"gen", "gen_sum", "gen_count", "gen_bucket"} <= set(names):
        problems.append(f"otel metrics: {names}")
    if rejections("logit-hec-relay"):
        problems.append(f"sink logged {len(rejections('logit-hec-relay'))} rejection(s)")
    detail = f"{len(replay)} requests replayed, {len(replay) - len(bad)} 2xx; indexed {counts}; metrics {names}"
    return ("FAIL" if problems else "PASS"), "; ".join(problems + [detail])


LEGS = [
    ("hec-logs", "structured logs -> splunk_hec_out -> main", leg_logs),
    ("hec-metrics", "every metric kind, multi_value: expand -> metrics", leg_metrics),
    ("hec-spans", "a span -> splunk_hec_out -> main", leg_spans),
    ("hec-ack", "logs on a useACK token, ack: true", leg_ack),
    ("hec-relay", "recorded captures -> splunk_hec_in -> splunk_hec_out -> Splunk", leg_relay),
]


# ---- probes -------------------------------------------------------------------------------------


def indexed(markers, index="main", wait=30):
    """Which of `markers` Splunk indexed, polled for `wait` seconds."""
    query = f'search index={index} ({" OR ".join(chr(34) + m + chr(34) for m in markers)}) | table _raw'
    deadline = time.monotonic() + wait
    while True:
        raws = [r.get("_raw", "") for r in search(query)]
        found = [m for m in markers if any(m in raw for raw in raws)]
        if len(found) == len(markers) or time.monotonic() > deadline:
            return found
        time.sleep(3)


def probe_version():
    info = rest_json("/services/server/info")
    version = info.get("entry", [{}])[0].get("content", {}).get("version")
    limits = rest_json("/services/configs/conf-limits/http_input")
    content = limits.get("entry", [{}])[0].get("content", {})
    return [("version and max_content_length", "INFO",
             f"Splunk {version}; limits.conf [http_input] max_content_length={content.get('max_content_length')}")]


def probe_gzip():
    m = f"{RUN}-gzip"
    event = hec("/services/collector/event", gzip.compress(objects({"event": f"{m}-event"})), headers={"Content-Encoding": "gzip"})
    raw = hec(f"/services/collector/raw?channel={uuid.uuid4()}", gzip.compress(f"{m}-raw\n".encode()),
              headers={"Content-Encoding": "gzip"})
    deflate = hec("/services/collector/event", zlib.compress(objects({"event": f"{m}-deflate"})),
                  headers={"Content-Encoding": "deflate"})
    found = indexed([f"{m}-event", f"{m}-raw"])
    return [("gzip on /event and /raw", "INFO",
             f"/event -> {event[0]} {event[1]}, /raw -> {raw[0]} {raw[1]}; indexed {found}; "
             f"deflate on /event -> {deflate[0]} {deflate[1][:80]!r}")]


def probe_code6():
    rows = []
    cases = [
        ("syntax error in object 1", lambda m: (json.dumps({"event": f"{m}-a"}) + '{"event": "' + f"{m}-b" + '", '
                                                 + json.dumps({"event": f"{m}-c"})).encode()),
        ("syntax error in object 0", lambda m: ('{"event": ' + json.dumps({"event": f"{m}-a"})).encode()),
        ("blank event in object 1", lambda m: objects({"event": f"{m}-a"}, {"event": ""}, {"event": f"{m}-c"})),
        ("unknown index in object 1", lambda m: objects({"event": f"{m}-a"}, {"event": f"{m}-b", "index": "nope"},
                                                        {"event": f"{m}-c"})),
        ("nested fields in object 1", lambda m: objects({"event": f"{m}-a"}, {"event": f"{m}-b", "fields": {"n": {"x": 1}}},
                                                        {"event": f"{m}-c"})),
        ("fields but no event in object 1", lambda m: objects({"event": f"{m}-a"}, {"fields": {"x": 1}},
                                                              {"event": f"{m}-c"})),
        ("neither event nor fields in object 1", lambda m: objects({"event": f"{m}-a"}, {"time": 1},
                                                                   {"event": f"{m}-c"})),
        ("unknown envelope key in object 1", lambda m: objects({"event": f"{m}-a"}, {"event": f"{m}-b", "wat": 1},
                                                               {"event": f"{m}-c"})),
        ("unknown envelope key in its only object", lambda m: objects({"event": f"{m}-a", "wat": 1})),
    ]
    for i, (label, body) in enumerate(cases):
        m = f"{RUN}-c{i}"
        status, text = hec("/services/collector/event", body(m))
        found = indexed([f"{m}-a", f"{m}-b", f"{m}-c"], wait=30)
        rows.append((f"batch with {label}", "INFO", f"{status} {text}; indexed {[f.rsplit('-', 1)[1] for f in found]}"))
    return rows


def probe_metrics():
    rows = []
    m = f"{RUN}-mt"
    hec("/services/collector/event", objects({"event": "metric", "index": "metrics",
                                              "fields": {"probe": m, "metric_name:probe.mtype": 3, "metric_type": "Sum"}}))
    sc4s = hec("/services/collector/event", objects({"time": str(int(time.time())), "index": "metrics", "source": "sc4s",
                                                     "fields": {"probe": m, "metric_name:probe.sc4s": "5"}}))
    single = hec("/services/collector/event", objects({"event": "metric", "index": "metrics",
                                                       "fields": {"probe": m, "metric_name": "probe.single", "_value": 7}}))
    for n in (200, 1000):
        fields = {"probe": m, f"metric_name:probe.dims{n}": n, **{f"d{i}": f"v{i}" for i in range(n)}}
        status, text = hec("/services/collector/event", objects({"event": "metric", "index": "metrics", "fields": fields}))
        dims = search_until(f'| mcatalog values(_dims) as d where index=metrics metric_name="probe.dims{n}"',
                            lambda r: bool(r) and bool(r[0].get("d")), 30)
        count = len(as_list(dims[0].get("d"))) if dims else 0
        rows.append((f"metric event with {n} dimensions", "INFO", f"{status} {text}; mcatalog sees {count} dimensions"))
    stored = search_until('| mstats latest(_value) as v where index=metrics'
                          ' metric_name IN ("probe.mtype", "probe.sc4s", "probe.single") by metric_name',
                          lambda r: len(r) >= 3, 30)
    by_type = search(f'| mstats latest(_value) as v where index=metrics metric_name="probe.mtype" by metric_type')
    rows.append(("metric_type", "INFO", f"mstats by metric_type {[(r.get('metric_type'), r.get('v')) for r in by_type]}"))
    rows.append(("metric object forms", "INFO",
                 f"no `event` + string value -> {sc4s[0]}; single-metric form -> {single[0]}; stored "
                 f"{[(r.get('metric_name'), r.get('v')) for r in stored]}"))
    return rows


def probe_http():
    rows = []
    for path in ("/services/collector/event/1.0", "/services/collector/raw", "/services/collector/ack",
                 "/services/collector/health"):
        status, headers, body = request("OPTIONS", HEC + path)
        rows.append((f"OPTIONS {path}", "INFO", f"{status} Allow={headers.get('Allow')!r} body={body!r}"))
    for label, method, path in (("unknown path", "POST", "/services/collector/nope"),
                                ("GET on /event", "GET", "/services/collector/event")):
        status, _, body = request(method, HEC + path, b"{}" if method == "POST" else None,
                                  {"Authorization": f"Splunk {TOKEN}"})
        rows.append((label, "INFO", f"{status} {body}"))
    status, text = hec("/services/collector/raw?sourcetype=probe", f"{RUN}-raw-no-channel\n".encode())
    rows.append(("/raw with no channel, token without useACK", "INFO", f"{status} {text}"))
    return rows


def probe_ack():
    channel = str(uuid.uuid4())
    headers = {"X-Splunk-Request-Channel": channel}
    no_channel = hec("/services/collector/event", objects({"event": f"{RUN}-ack-a"}), token=ACK_TOKEN)
    first = hec("/services/collector/event", objects({"event": f"{RUN}-ack-b"}), token=ACK_TOKEN, headers=headers)
    second = hec("/services/collector/event", objects({"event": f"{RUN}-ack-c"}), token=ACK_TOKEN, headers=headers)
    ack_id = json.loads(first[1]).get("ackId")
    started, reply = time.monotonic(), ""
    while time.monotonic() - started < 60:
        _, reply = hec("/services/collector/ack", json.dumps({"acks": [ack_id]}).encode(), token=ACK_TOKEN, headers=headers)
        if "true" in reply:
            break
        time.sleep(1)
    took = time.monotonic() - started
    other = hec("/services/collector/ack", json.dumps({"acks": [ack_id]}).encode(), token=ACK_TOKEN,
                headers={"X-Splunk-Request-Channel": str(uuid.uuid4())})
    return [("useACK answers", "INFO",
             f"no channel -> {no_channel[0]} {no_channel[1]}; a new channel's first two -> {first[1]}, {second[1]}; "
             f"poll -> {reply} after {took:.1f}s; the same id on another channel -> {other[0]} {other[1]}")]


def probe_tcpout():
    m = f"{RUN}-tcpout"
    for obj in ({"event": f"{m} first line"}, {"event": f"{m} second line\nwith an embedded newline"},
                {"event": {"structured": m, "n": 1}, "fields": {"f": "v"}}):
        hec("/services/collector/event", objects({**obj, "sourcetype": "tcpout:probe"}), token=TCPOUT_TOKEN)
    hec("/services/collector/raw?sourcetype=tcpout:raw", f"{m} raw one\n{m} raw two\n".encode(), token=TCPOUT_TOKEN)
    # rawcap writes one file per connection, closing each after 5 s of quiet; Splunk reconnects.
    deadline = time.monotonic() + 180
    while True:
        files = sorted((OUT / "tcpout").glob("tcpout-*.raw"))
        data = b"".join(path.read_bytes() for path in files)
        if m.encode() + b" raw two" in data or time.monotonic() > deadline:
            break
        time.sleep(3)
    lines = data.split(b"\n")
    ours = [line.decode(errors="replace") for line in lines if m.encode() in line or line == b"with an embedded newline"]
    other = sum(1 for line in lines if line and m.encode() not in line)
    return [("[tcpout] sendCookedData=false", "INFO",
             f"{len(files)} connection(s), {len(data)} bytes; this run's lines as received, in order: {ours}; "
             f"{other} other LF-terminated lines (Splunk's own logs)")]


PROBES = [probe_version, probe_gzip, probe_code6, probe_metrics, probe_http, probe_ack, probe_tcpout]


# ---- main ---------------------------------------------------------------------------------------


def main():
    results = []
    for leg, title, check in LEGS:
        try:
            result, detail = check()
        except Exception as err:  # A query that failed is a failed leg, not a crashed harness.
            result, detail = "FAIL", f"{type(err).__name__}: {err}"
        results.append({"leg": leg, "title": title, "result": result, "detail": detail})

    probe_rows = []
    for probe in PROBES:
        try:
            probe_rows += probe()
        except Exception as err:
            probe_rows.append((probe.__name__, "FAIL", f"{type(err).__name__}: {err}"))

    cell = lambda text: str(text).replace("|", "\\|").replace("\n", "\\n")
    lines = ["| Leg | What | Result | Detail |", "|---|---|---|---|"]
    lines += [f"| {r['leg']} | {r['title']} | {r['result']} | {cell(r['detail'])} |" for r in results]
    lines += ["", "| Probe | Result | Detail |", "|---|---|---|"]
    lines += [f"| {probe} | {result} | {cell(detail)} |" for probe, result, detail in probe_rows]
    table = "\n".join(lines)
    print(table)
    (OUT / "results.md").write_text(table + "\n")
    (OUT / "results.json").write_text(json.dumps(
        {"legs": results, "probes": [dict(zip(("probe", "result", "detail"), r)) for r in probe_rows]},
        indent=2) + "\n")
    failed = any(r["result"] == "FAIL" for r in results) or any(r[1] == "FAIL" for r in probe_rows)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
