#!/usr/bin/env python3
"""Confirms each leg of `script/victoria-interop` against the backend it wrote to.

Runs in a throwaway `python:3.12-slim` container on the stack's network, stdlib only, after the
harness has held the stack up for its window and copied every service's log into the run
directory. Mounts:

    /out       the run directory: logs/<service>.log, plus the files `logit-federate.yaml` and
               `logit-vmagent-in.yaml` write; results.md and results.json land here
    /fixtures  testdata/interop/prometheus, read-only, for the replay probes

Prints one row per leg (docs/plans/victoriametrics-interop.md, "W1's legs") and one per probe.
A leg is PASS (it did what the plan expects), GAP (the backend or `logit` behaves in a way the
plan's "Findings" records as a gap), or FAIL (nothing arrived, or something unexpected did). Exits
1 on any FAIL.
"""

import json
import pathlib
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

VM = "http://victoria-metrics:8428"
VL = "http://victoria-logs:9428"
VT = "http://victoria-traces:10428"

OUT = pathlib.Path("/out")
FIXTURES = pathlib.Path("/fixtures")


# ---- HTTP ---------------------------------------------------------------------------------------


def get(url):
    with urllib.request.urlopen(url, timeout=15) as response:
        return response.read().decode("utf-8", errors="replace")


def get_json(url):
    return json.loads(get(url))


def post(url, body, headers):
    request = urllib.request.Request(url, data=body, method="POST", headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            return response.status, response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as err:
        return err.code, err.read().decode("utf-8", errors="replace")


def q(params):
    return urllib.parse.urlencode(params, doseq=True)


# ---- backend queries ----------------------------------------------------------------------------


def vm_series(selector):
    """Every series VictoriaMetrics holds for `selector`, over all time."""
    return get_json(f"{VM}/api/v1/series?" + q({"match[]": selector, "start": "0"}))["data"]


def vm_export(selector):
    """`/api/v1/export`'s JSON lines for `selector`: one {"metric", "values", "timestamps"} each."""
    body = get(f"{VM}/api/v1/export?" + q({"match[]": selector, "start": "0"}))
    return [json.loads(line) for line in body.splitlines() if line.strip()]


def vl_query(logsql, limit=5):
    body = get(f"{VL}/select/logsql/query?" + q({"query": logsql, "limit": str(limit)}))
    return [json.loads(line) for line in body.splitlines() if line.strip()]


def vl_count(logsql):
    rows = vl_query(f"{logsql} | stats count() as n", limit=1)
    return int(rows[0]["n"]) if rows else 0


def vt_traces(service):
    url = f"{VT}/select/jaeger/api/traces?" + q({"service": service, "limit": "20"})
    return get_json(url).get("data") or []


def names(series):
    return sorted({s["__name__"] for s in series})


def label_names(series):
    return sorted({k for s in series for k in s if k != "__name__"})


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
    events = []
    attrs = ""
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
        if not metric_line.startswith(f"metric  {metric} "):
            continue
        if all(f'{k}="{v}"' in attr_line for k, v in attrs.items()):
            match = re.search(r"sum=([0-9.e+-]+)", metric_line)
            if match:
                total += float(match.group(1))
    return int(total)


# ---- legs ---------------------------------------------------------------------------------------


def leg_1():
    series = vm_series('{__name__=~"vi_rw1_.*"}')
    got = names(series)
    want = ["vi_rw1_gauge", "vi_rw1_requests_total"]
    if got != want:
        return "FAIL", f"want {want} in VictoriaMetrics, have {got}"
    points = sum(len(row["values"]) for row in vm_export('{__name__=~"vi_rw1_.*"}'))
    return "PASS", f"{', '.join(got)} stored ({points} points), labels {label_names(series)}"


def leg_1z():
    series = vm_series('{__name__=~"vi_rw1z_.*"}')
    got = names(series)
    want = ["vi_rw1z_gauge", "vi_rw1z_requests_total"]
    if got != want:
        return "FAIL", f"want {want} in VictoriaMetrics, have {got}"
    points = sum(len(row["values"]) for row in vm_export('{__name__=~"vi_rw1z_.*"}'))
    return "PASS", f"{', '.join(got)} stored ({points} points) via compression: zstd, labels {label_names(series)}"


def leg_2():
    stored = names(vm_series('{__name__=~"vi_rw2_.*"}'))
    logged = strip_ansi(log("logit-rw2"))
    rejected = re.search(r"remote_write_rejected|rejected", logged)
    if stored:
        return "FAIL", f"VictoriaMetrics stored {stored} from a 2.0 sender"
    if rejected:
        return "PASS", "refused: " + rejected.group(0)
    return "GAP", (
        "VictoriaMetrics answered every 2.0 request 2xx and stored nothing; prometheus_out logged "
        "no rejection (see the probe row for the status)"
    )


def leg_3():
    series = vm_series('{__name__=~"vi_expose_.*",job="logit-expose"}')
    got = names(series)
    want = ["vi_expose_gauge", "vi_expose_requests_total"]
    if got != want:
        return "FAIL", f"want {want} with job=logit-expose, have {got}"
    return "PASS", f"{', '.join(got)} scraped by vmagent, labels {label_names(series)}"


def leg_4():
    series = vm_series('{__name__=~"vi_influx_.*"}')
    if not series:
        return "FAIL", "no vi_influx_* series in VictoriaMetrics"
    labels = label_names(series)
    carried = [label for label in ("org", "bucket", "db") if label in labels]
    return "PASS", (
        f"{', '.join(names(series))}, labels {labels}; of org/bucket/db, "
        f"{carried or 'none'} became labels"
    )


def leg_5():
    series = vm_series('{__name__="vi_graphite_gauge"}')
    if not series:
        return "FAIL", "no vi_graphite_gauge in VictoriaMetrics"
    if not any(s.get("leg") == "graphite" for s in series):
        return "FAIL", f"the carbon tag did not become a label: {series}"
    return "PASS", f"vi_graphite_gauge, labels {label_names(series)} (the ;leg= tag is a label)"


def leg_6():
    problems, notes = [], []

    series = vm_series('{__name__=~"vi_otlp_.*"}')
    got = names(series)
    for want in ("vi_otlp_delta_sum", "vi_otlp_cumulative_sum", "vi_otlp_exphist_bucket"):
        if want not in got:
            problems.append(f"no {want}")
    delta = [row for row in vm_export('{__name__="vi_otlp_delta_sum"}')]
    delta_values = sorted({v for row in delta for v in row["values"]})
    vmranges = sorted({s["vmrange"] for s in series if "vmrange" in s})
    notes.append(f"metrics {got}; delta sum stored as values {delta_values}; "
                 f"exphist as {len(vmranges)} vmrange buckets")

    for sink in ("default", "msg-field"):
        rows = vl_query(f"vi_sink:{sink}", limit=1)
        if not rows:
            problems.append(f"no VictoriaLogs records from the {sink} sink")
            continue
        msg = rows[0].get("_msg", "")
        if not msg.startswith("victoria-interop otlp-http log"):
            problems.append(f"{sink} sink: _msg is {msg!r}")
        notes.append(f"logs[{sink}] _msg={msg!r} _stream={rows[0].get('_stream')}")

    traces = vt_traces("victoria-interop-otlp-http")
    spans = [s["operationName"] for t in traces for s in t["spans"]]
    if "vi-otlp-http-span" not in spans:
        problems.append("no vi-otlp-http-span in VictoriaTraces")
    notes.append(f"traces: {len(traces)} with vi-otlp-http-span")

    return ("FAIL" if problems else "PASS"), "; ".join(problems + notes)


def leg_7():
    traces = vt_traces("victoria-interop-otlp-grpc")
    spans = [s["operationName"] for t in traces for s in t["spans"]]
    failures = len(re.findall(r'key="?send_failed', strip_ansi(log("logit-otlp-grpc"))))
    if "vi-otlp-grpc-span" not in spans:
        return "FAIL", "no vi-otlp-grpc-span in VictoriaTraces"
    detail = f"{len(traces)} traces with vi-otlp-grpc-span in VictoriaTraces"
    if failures:
        return "GAP", (
            f"{detail}, but otlp_out logged {failures} send_failed line(s): the listener closes "
            "each connection mid-stream and the in-flight batch is dropped"
        )
    return "PASS", detail


def leg_8():
    rows = vl_query("app_name:vi-syslog", limit=1)
    if not rows:
        return "FAIL", "no app_name:vi-syslog records in VictoriaLogs"
    msg = rows[0].get("_msg", "")
    if not msg.startswith("victoria-interop syslog line"):
        return "FAIL", f"_msg is {msg!r}: the octet-count framing was not detected"
    return "PASS", (
        f"{vl_count('app_name:vi-syslog')} records, _msg={msg!r} (no length prefix: octet "
        f"counting detected), format={rows[0].get('format')}, _stream={rows[0].get('_stream')}"
    )


def leg_9():
    events = rendered_events(OUT / "federate.log")
    ours = [(a, m) for a, m in events if m.startswith("metric  vi_rw1_gauge ")]
    if not ours:
        return "FAIL", "prometheus_in scraped no vi_rw1_gauge from /federate"
    attrs, metric = ours[-1]
    kind = re.search(r'prometheus\.type="([^"]+)"', attrs)
    stamped = "prometheus.timestamp=true" in attrs
    return "PASS", (
        f"{len(ours)} scrapes of vi_rw1_gauge, rendered `{metric.split(None, 1)[1]}`, "
        f"prometheus.type={kind.group(1) if kind else 'absent'}, "
        f"wire timestamp {'kept' if stamped else 'absent'}"
    )


def leg_10():
    # After W2, `prometheus_in` accepts zstd on the first request, so vmagent (zstd by default)
    # never gets the `415` that used to force its Snappy downgrade: every write is class=ok,
    # encoding=zstd, and class=unsupported stays 0.
    telemetry = rendered_events(OUT / "vmagent-in-telemetry.log")
    unsupported = telemetry_sum(telemetry, "logit.input.writes", **{"class": "unsupported"})
    ok = telemetry_sum(telemetry, "logit.input.writes", **{"class": "ok"})
    ok_zstd = telemetry_sum(telemetry, "logit.input.writes", **{"class": "ok", "encoding": "zstd"})
    skipped = telemetry_sum(telemetry, "logit.input.metrics.skipped")
    received = rendered_events(OUT / "vmagent-in-received.log")
    ours = sorted({m.split()[1] for _, m in received if m.split()[1].startswith("vi_expose_")})
    downgraded = "Downgrading protocol from VictoriaMetrics to Prometheus" in log("vmagent")

    detail = (
        f"writes class=ok,encoding=zstd {ok_zstd} (class=ok total {ok}), class=unsupported "
        f"{unsupported}; vmagent log {'shows' if downgraded else 'does not show'} the downgrade; "
        f"received {ours or 'no'} vi_expose_* series; {skipped} series skipped"
    )
    if not (ok_zstd >= 1 and unsupported == 0 and not downgraded):
        return "FAIL", detail
    if not ours or skipped:
        return "GAP", detail
    return "PASS", detail


LEGS = [
    (1, "prometheus_out version: 1 -> VictoriaMetrics /api/v1/write", leg_1),
    ("1z", "prometheus_out version: 1, compression: zstd -> VictoriaMetrics /api/v1/write", leg_1z),
    (2, "prometheus_out version: 2 -> VictoriaMetrics", leg_2),
    (3, "prometheus_out bind: <- vmagent scrape -> VictoriaMetrics", leg_3),
    (4, "influxdb_out -> VictoriaMetrics /api/v2/write", leg_4),
    (5, "graphite_out plaintext, tags: carbon -> VictoriaMetrics :2003", leg_5),
    (6, "otlp_out HTTP -> VictoriaMetrics, VictoriaLogs, VictoriaTraces", leg_6),
    (7, "otlp_out gRPC -> VictoriaTraces", leg_7),
    (8, "syslog_out TCP -> VictoriaLogs syslog", leg_8),
    (9, "prometheus_in scrape <- VictoriaMetrics /federate", leg_9),
    (10, "vmagent remote-write -> prometheus_in bind:", leg_10),
]


# ---- probes -------------------------------------------------------------------------------------


def replay(fixture, probe, headers):
    """POSTs a committed capture's body to VictoriaMetrics under `extra_label=probe=<probe>`, so
    what it stored can be told apart from every other write."""
    body = (FIXTURES / f"{fixture}.bin").read_bytes()
    url = f"{VM}/api/v1/write?" + q({"extra_label": f"probe={probe}"})
    return post(url, body, headers)


def probes():
    rows = []
    v1 = {"Content-Type": "application/x-protobuf"}
    variants = [
        ("prometheus-v2-000", "v2", {
            "Content-Type": "application/x-protobuf;proto=io.prometheus.write.v2.Request",
            "Content-Encoding": "snappy", "X-Prometheus-Remote-Write-Version": "2.0.0"}),
        ("vmagent-zstd-000", "zstd_as_sent", {**v1, "Content-Encoding": "zstd",
                                              "X-VictoriaMetrics-Remote-Write-Version": "1"}),
        ("vmagent-zstd-000", "zstd_no_version", {**v1, "Content-Encoding": "zstd"}),
        ("vmagent-zstd-000", "zstd_prom_version", {**v1, "Content-Encoding": "zstd",
                                                   "X-Prometheus-Remote-Write-Version": "0.1.0"}),
        ("vmagent-zstd-000", "zstd_labelled_snappy", {**v1, "Content-Encoding": "snappy"}),
        ("vmagent-zstd-000", "zstd_no_encoding", v1),
        ("vmagent-snappy-001", "snappy_labelled_zstd", {**v1, "Content-Encoding": "zstd"}),
    ]
    statuses = {}
    for fixture, probe, headers in variants:
        if not (FIXTURES / f"{fixture}.bin").exists():
            rows.append((probe, "SKIP", f"no fixture {fixture}.bin"))
            continue
        statuses[probe] = (fixture, replay(fixture, probe, headers))
    # A write reaches search a few seconds after its 2xx, and a probe that was dropped never
    # does, so poll for a bounded window rather than read once.
    stored = []
    for _ in range(20):
        stored = get_json(f"{VM}/api/v1/label/probe/values?start=0")["data"]
        if all(probe in stored for probe in statuses):
            break
        time.sleep(1)
    for probe, (fixture, (status, body)) in statuses.items():
        verdict = "stored" if probe in stored else "not stored"
        rows.append((probe, "INFO", f"{fixture}.bin -> {status} {body.strip()[:120]!r}, {verdict}"))
    return rows


# ---- main ---------------------------------------------------------------------------------------


def main():
    results = []
    for number, title, check in LEGS:
        try:
            result, detail = check()
        except Exception as err:  # A query that failed is a failed leg, not a crashed harness.
            result, detail = "FAIL", f"{type(err).__name__}: {err}"
        results.append({"leg": number, "title": title, "result": result, "detail": detail})

    probe_rows = []
    try:
        probe_rows = probes()
    except Exception as err:
        probe_rows = [("probes", "FAIL", f"{type(err).__name__}: {err}")]

    lines = ["| Leg | What | Result | Detail |", "|---|---|---|---|"]
    for row in results:
        lines.append(f"| {row['leg']} | {row['title']} | {row['result']} | {row['detail']} |")
    lines += ["", "| Probe | Result | Detail |", "|---|---|---|"]
    for probe, result, detail in probe_rows:
        lines.append(f"| {probe} | {result} | {detail} |")
    table = "\n".join(lines)
    print(table)

    (OUT / "results.md").write_text(table + "\n")
    (OUT / "results.json").write_text(json.dumps(
        {"legs": results, "probes": [dict(zip(("probe", "result", "detail"), r)) for r in probe_rows]},
        indent=2) + "\n")

    return 1 if any(row["result"] == "FAIL" for row in results) else 0


if __name__ == "__main__":
    sys.exit(main())
