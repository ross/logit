#!/usr/bin/env python3
"""Confirms each leg of `script/splunk-interop` against Splunk, then probes Splunk directly.

Runs in a throwaway `python:3.12-slim` container on the stack's network, stdlib only, after the
harness has held the legs up for its window and copied every service's log into the run
directory. Mounts:

    /out  the run directory: logs/<service>.log, replay.log, <leg>-telemetry.log (each leg's
          sink telemetry), tcpout/;
          results.md, results.json, and search.spl land here

The target comes from the environment, defaulting to the local stack (the README's "Splunk Cloud
mode" lists the variables): `SPLUNK_INTEROP_TARGET` is `local` or `cloud`, the probes post to
HEC at `SPLUNK_INTEROP_HEC_URL`, and `SPLUNK_INTEROP_SEARCH` picks how a leg is confirmed.
`rest` searches Splunk's REST API (`/services/search/jobs/export`, `output_mode=json`) with
`SPLUNK_INTEROP_API_AUTH`; `none`, cloud mode's default, searches nothing. An event search is
bounded by index time to `SPLUNK_INTEROP_SINCE` rather than by event time, since the relay leg's
recorded events carry their recorded timestamps; an `mstats` or `mcatalog` query is unbounded.

A leg is PASS (Splunk holds what the leg sent, in the shape docs/plans/splunk-relay.md expects),
GAP (it arrived, with a difference the README records), SENT (under `none`: the sink's
telemetry shows requests answered 2xx with records and nothing dropped, and the row carries the
SPL that would confirm arrival), SKIP (the target can't run it),
or FAIL. A probe row is INFO, what Splunk answered, or SKIP. Every SPL query a leg or probe runs,
or would run, lands in search.spl for a manual search pass. results.md, results.json, and
search.spl have `SPLUNK_INTEROP_STACK` and the configured tokens scrubbed. Exits 1 on any FAIL.
"""

import base64
import gzip
import json
import os
import pathlib
import re
import ssl
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
import zlib

TARGET = os.environ.get("SPLUNK_INTEROP_TARGET", "local")
LOCAL = TARGET == "local"


def setting(name, local_default):
    """`name` from the environment; unset, the local stack's value, or empty in cloud mode."""
    value = os.environ.get(name)
    if value is None:
        return local_default if LOCAL else ""
    return value


HEC_URL = setting("SPLUNK_INTEROP_HEC_URL", "http://splunk:8088/services/collector")
HEC_URL_ALT = os.environ.get("SPLUNK_INTEROP_HEC_URL_ALT", "")


def hec_base(url):
    """A HEC URL without its trailing `/services/collector`: the probes append whole routes."""
    return re.sub(r"/services/collector$", "", url.rstrip("/"))


HEC = hec_base(HEC_URL)
API = setting("SPLUNK_INTEROP_API_URL", "https://splunk:8089")
TOKEN = setting("SPLUNK_INTEROP_HEC_TOKEN", "11111111-1111-1111-1111-111111111111")
ACK_TOKEN = setting("SPLUNK_INTEROP_ACK_TOKEN", "22222222-2222-2222-2222-222222222222")
TCPOUT_TOKEN = "44444444-4444-4444-4444-444444444444"
ADMIN = setting("SPLUNK_INTEROP_API_AUTH", "Basic " + base64.b64encode(b"admin:logit-splunk-interop").decode())
SEARCH = os.environ.get("SPLUNK_INTEROP_SEARCH", "rest" if LOCAL else "none")
SINCE = os.environ.get("SPLUNK_INTEROP_SINCE", "0")
STACK = os.environ.get("SPLUNK_INTEROP_STACK", "")
# Splunk's management port serves a self-signed certificate, and so does a trial stack's HEC.
INSECURE = ssl._create_unverified_context()

OUT = pathlib.Path("/out")
RUN = f"r{int(time.time())}"

#: What `indexed()` and the probes' searches return under `SPLUNK_INTEROP_SEARCH=none`.
NOT_SEARCHED = "not searched"


class SearchUnavailable(Exception):
    """`SPLUNK_INTEROP_SEARCH=none`: there is no search transport to confirm a leg with."""


#: (leg or probe, SPL) for search.spl, in the order they ran, without repeats.
SPL = []
#: The leg or probe running now, which `search()` files its query under.
CURRENT = ["main"]


def record_spl(query, label=None):
    entry = (label or CURRENT[0], query)
    if entry not in SPL:
        SPL.append(entry)


def scrub(text):
    """`text` without the stack name or any configured credential."""
    if STACK:
        text = text.replace(STACK, "<stack>")
    for secret in (TOKEN, ACK_TOKEN, TCPOUT_TOKEN, ADMIN):
        if secret:
            text = text.replace(secret, "<redacted>")
    return text


# ---- HTTP ---------------------------------------------------------------------------------------


def request(method, url, body=None, headers=None):
    """(status, headers, body text); an HTTP error status is a result, not an exception."""
    req = urllib.request.Request(url, data=body, method=method, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=30, context=INSECURE) as response:
            return response.status, response.headers, response.read().decode(errors="replace")
    except urllib.error.HTTPError as err:
        return err.code, err.headers, err.read().decode(errors="replace")


def hec_full(path, body, token=TOKEN, headers=None):
    all_headers = {"Authorization": f"Splunk {token}", **(headers or {})}
    return request("POST", HEC + path, body, all_headers)


def hec(path, body, token=TOKEN, headers=None):
    status, _, text = hec_full(path, body, token, headers)
    return status, text


def objects(*objs):
    return "".join(json.dumps(o) for o in objs).encode()


def index_refused(text):
    """Whether a HEC reply is code 7, an index the token may not write (a trial may lack it)."""
    try:
        return json.loads(text).get("code") == 7
    except (ValueError, AttributeError):
        return False


#: The generating commands `search()` leaves without an index-time bound.
METRIC_COMMANDS = ("| mstats", "| mcatalog")


def search(query):
    """Result rows of one export search over everything indexed since `SINCE`."""
    record_spl(query)
    if SEARCH == "none":
        raise SearchUnavailable(query)
    if not API:
        raise RuntimeError("SPLUNK_INTEROP_SEARCH=rest needs SPLUNK_INTEROP_API_URL")
    params = {"search": query, "output_mode": "json", "earliest_time": "0"}
    # `index_earliest` empties an `mstats` or `mcatalog` result on Splunk 10.4.3, so a metrics
    # query stays unbounded and can match an earlier run's series.
    if not query.startswith(METRIC_COMMANDS):
        params["index_earliest"] = SINCE
    body = urllib.parse.urlencode(params)
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


def try_search(query, enough=None, timeout=30):
    """`search`, or `search_until` given `enough`, but `NOT_SEARCHED` under `none`."""
    try:
        return search(query) if enough is None else search_until(query, enough, timeout)
    except SearchUnavailable:
        return NOT_SEARCHED


def rest_json(path):
    if SEARCH == "none":
        raise SearchUnavailable(path)
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


#: The diagnostic keys a sink logs when Splunk refused a request or the sink dropped records.
REJECTION_KEYS = ("request_rejected", "token_rejected", "send_failed", "invalid_event", "oversize")


def rejections(service):
    """The sink's warn-level diagnostics about a refused request or dropped records."""
    lines = strip_ansi(log(service)).splitlines()
    pattern = re.compile(r'key="?(' + "|".join(REJECTION_KEYS) + r')\b')
    return [line for line in lines if pattern.search(line)]


def delivery(leg):
    """The sink's own account of a leg from /out/<leg>-telemetry.log, as (problems, summary).

    A connect, DNS, or TLS failure is retried without a log line until the retry budget runs out,
    and a code 6 or oversize drop still returns success, so only the telemetry shows either.
    """
    events = rendered_events(OUT / f"{leg}-telemetry.log")
    requests = telemetry_sum(events, "logit.output.requests", route="event")
    ok = telemetry_sum(events, "logit.output.requests", route="event", **{"class": "2xx"})
    network = telemetry_sum(events, "logit.output.requests", route="event", **{"class": "network_error"})
    records = telemetry_sum(events, "logit.output.records")
    dropped = {reason: telemetry_sum(events, "logit.output.records.dropped", reason=reason)
               for reason in ("oversize", "invalid_event")}
    dropped_total = telemetry_sum(events, "logit.output.records.dropped")
    failed = telemetry_sum(events, "logit.component.batches.dropped", reason="send_failed")
    retries = telemetry_sum(events, "logit.component.retries")
    summary = (f"telemetry: {requests} /event requests, {ok} 2xx, {network} network_error, "
               f"{requests - ok - network} other; {records} records delivered; records dropped "
               f"{dropped_total} {dropped}; {failed} batches dropped send_failed; {retries} retries")
    problems = []
    if not events:
        problems.append(f"no {leg}-telemetry.log")
    if ok < 1 or records < 1:
        problems.append("no request answered 2xx with records")
    if dropped_total or failed:
        problems.append("the sink dropped records or batches")
    return problems, summary, ok, records


def replay_lines():
    path = OUT / "replay.log"
    return path.read_text().splitlines() if path.exists() else []


# ---- legs ---------------------------------------------------------------------------------------

LOGS_EVENT = ('search index=main sourcetype="logit:test" | head 1 | table _raw host source'
              ' otel.log.severity.text otel.log.severity.number trace_id span_id detail.stage detail.ok leg')
LOGS_TSTATS = '| tstats count where index=main sourcetype="logit:test" by detail.stage, otel.log.severity.number'
METRICS_SERIES = '| mstats latest(_value) as v where index=metrics metric_name="splunk_interop.*" by metric_name'
METRICS_TYPES = '| mcatalog values(metric_type) as t where index=metrics metric_name="splunk_interop.*"'
METRICS_BY_TYPE = '| mstats count(_value) as n where index=metrics metric_name="splunk_interop.*" by metric_type'
METRICS_BUCKETS = ('| mstats max(_value) as c where index=metrics metric_name="splunk_interop.histogram_bucket"'
                   ' by le')
METRICS_PERC = METRICS_BUCKETS + ' | `histperc(0.5, c, le)`'
SPANS_EVENT = 'search index=main sourcetype="logit:span" | head 1 | spath | table *'
ACK_COUNT = 'search index=main sourcetype="logit:ack" | stats count'
RELAY_METRICS = '| mstats latest(_value) where index=metrics metric_name="gen*" by metric_name'

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


def leg_logs():
    rows = search_until(LOGS_EVENT, lambda r: len(r) >= 1)
    if not rows:
        return "FAIL", "no sourcetype=logit:test events in main"
    row = rows[0]
    fields = {k: row.get(k) for k in ("host", "source", "otel.log.severity.text", "otel.log.severity.number",
                                      "trace_id", "span_id", "detail.stage", "detail.ok", "leg")}
    indexed = search(LOGS_TSTATS)
    problems = [k for k, v in fields.items() if v in (None, "")]
    if rejections("logit-hec-logs"):
        problems.append(f"sink logged {len(rejections('logit-hec-logs'))} rejection(s)")
    if not indexed:
        problems.append("tstats finds no indexed detail.stage")
    detail = f"_raw={row.get('_raw')!r}; fields {fields}; tstats by indexed fields: {indexed[:1]}"
    return ("FAIL" if problems else "PASS"), "; ".join(problems + [detail])


def leg_metrics():
    rows = search_until(METRICS_SERIES,
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
    types = search(METRICS_TYPES)
    by_type = search(METRICS_BY_TYPE)
    buckets = search(METRICS_BUCKETS)
    perc = search(METRICS_PERC)
    if rejections("logit-hec-metrics"):
        problems.append(f"sink logged {len(rejections('logit-hec-metrics'))} rejection(s)")
    detail = (f"{len(names)} series: {names}; metric_type values {types[:1]}; count by metric_type "
              f"{[(r.get('metric_type'), r.get('n')) for r in by_type]}; histogram buckets "
              f"{[(r.get('le'), r.get('c')) for r in buckets]}; histperc(0.5) {perc[:1]}")
    return ("FAIL" if problems else "PASS"), "; ".join(problems + [detail])


def leg_spans():
    rows = search_until(SPANS_EVENT, lambda r: len(r) >= 1)
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
    if not ACK_TOKEN:
        return "SKIP", "no SPLUNK_INTEROP_ACK_TOKEN: the leg didn't run"
    telemetry = rendered_events(OUT / "hec-ack-telemetry.log")
    acked = telemetry_sum(telemetry, "logit.output.acks", result="acked")
    timeout = telemetry_sum(telemetry, "logit.output.acks", result="timeout")
    unsupported = telemetry_sum(telemetry, "logit.output.acks", result="unsupported")
    polls = telemetry_sum(telemetry, "logit.output.requests", route="ack")
    acks = f"acks acked={acked} timeout={timeout} unsupported={unsupported}; {polls} /ack polls"
    confirmed = acked >= 1 and timeout == 0 and unsupported == 0
    try:
        rows = search(ACK_COUNT)
    except SearchUnavailable:
        problems, summary, _, _ = delivery("hec-ack")
        verdict = "SENT" if confirmed and not problems else "FAIL"
        return verdict, "; ".join(problems + [acks, summary, f"not searched: {ACK_COUNT}"])
    count = int(rows[0]["count"]) if rows else 0
    detail = f"{acks}; {count} events indexed"
    return ("PASS" if confirmed and count >= 1 else "FAIL"), detail


def leg_relay():
    replay = replay_lines()
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
    metrics = search(RELAY_METRICS)
    names = sorted(r["metric_name"] for r in metrics)
    if not {"gen", "gen_sum", "gen_count", "gen_bucket"} <= set(names):
        problems.append(f"otel metrics: {names}")
    if rejections("logit-hec-relay"):
        problems.append(f"sink logged {len(rejections('logit-hec-relay'))} rejection(s)")
    detail = f"{len(replay)} requests replayed, {len(replay) - len(bad)} 2xx; indexed {counts}; metrics {names}"
    return ("FAIL" if problems else "PASS"), "; ".join(problems + [detail])


#: (leg, what it sends, check, its logit service, the SPL that confirms it).
LEGS = [
    ("hec-logs", "structured logs -> splunk_hec_out -> main", leg_logs, "logit-hec-logs",
     [LOGS_EVENT, LOGS_TSTATS]),
    ("hec-metrics", "every metric kind, multi_value: expand -> metrics", leg_metrics, "logit-hec-metrics",
     [METRICS_SERIES, METRICS_TYPES, METRICS_BY_TYPE, METRICS_BUCKETS, METRICS_PERC]),
    ("hec-spans", "a span -> splunk_hec_out -> main", leg_spans, "logit-hec-spans", [SPANS_EVENT]),
    ("hec-ack", "logs on a useACK token, ack: true", leg_ack, "logit-hec-ack", [ACK_COUNT]),
    ("hec-relay", "recorded captures -> splunk_hec_in -> splunk_hec_out -> Splunk", leg_relay, "logit-hec-relay",
     [query for _, query, _ in RELAY_EXPECTED] + [RELAY_METRICS]),
]


def sent(leg, service, queries):
    """A leg's row when nothing can search: SENT if its sink's telemetry shows records answered
    2xx and nothing dropped, and its log no rejection; else FAIL."""
    problems, summary, ok, records = delivery(leg)
    facts = [f"the sink's telemetry shows {ok} requests answered 2xx for {records} records with "
             "nothing dropped; arrival unconfirmed by search"]
    refused = rejections(service)
    if refused:
        problems.append(f"sink logged {len(refused)} rejection(s), the first: {refused[0].strip()[:300]}")
    if service == "logit-hec-relay":
        replay = replay_lines()
        bad = [line for line in replay if not line.startswith("2")]
        if not replay:
            problems.append("replay.log is missing")
        elif bad:
            problems.append(f"non-2xx replies: {bad}")
        else:
            facts.append(f"all {len(replay)} replayed requests 2xx")
    would = " || ".join(queries)
    if problems:
        return "FAIL", "; ".join(problems + [summary, f"would search: {would}"])
    return "SENT", "; ".join(facts + [summary, f"not searched: {would}"])


# ---- probes -------------------------------------------------------------------------------------


def indexed(markers, index="main", wait=30):
    """Which of `markers` Splunk indexed, polled for `wait` seconds; `NOT_SEARCHED` under `none`."""
    query = f'search index={index} ({" OR ".join(chr(34) + m + chr(34) for m in markers)}) | table _raw'
    deadline = time.monotonic() + wait
    while True:
        try:
            rows = search(query)
        except SearchUnavailable:
            return NOT_SEARCHED
        raws = [r.get("_raw", "") for r in rows]
        found = [m for m in markers if any(m in raw for raw in raws)]
        if len(found) == len(markers) or time.monotonic() > deadline:
            return found
        time.sleep(3)


def probe_version():
    if SEARCH == "none":
        return [("version and max_content_length", "SKIP", "no REST API (SPLUNK_INTEROP_SEARCH=none)")]
    info = rest_json("/services/server/info")
    version = info.get("entry", [{}])[0].get("content", {}).get("version")
    limits = rest_json("/services/configs/conf-limits/http_input")
    content = limits.get("entry", [{}])[0].get("content", {})
    return [("version and max_content_length", "INFO",
             f"Splunk {version}; limits.conf [http_input] max_content_length={content.get('max_content_length')}")]


def certificate(host, port):
    """The leaf certificate's subject and issuer common names, fetched without verifying it."""
    pem = ssl.get_server_certificate((host, port), timeout=15)
    with tempfile.NamedTemporaryFile("w", suffix=".pem") as file:
        file.write(pem)
        file.flush()
        decoded = ssl._ssl._test_decode_cert(file.name)
    name = lambda rdns: ", ".join(f"{k}={v}" for rdn in rdns for k, v in rdn if k in ("commonName", "organizationName"))
    return f"subject {name(decoded.get('subject', ()))!r}, issuer {name(decoded.get('issuer', ()))!r}"


def endpoint_row(label, url):
    base = hec_base(url)
    health = base + "/services/collector/health"
    parsed = urllib.parse.urlsplit(base)
    status, _, body = request("GET", health)
    # probe_health_token has the token cases.
    facts = [f"/health without a token -> {status} {body[:120]!r}"]
    if parsed.scheme != "https":
        return (f"endpoint {label}", "INFO", "plain http; " + facts[0])
    try:
        req = urllib.request.Request(health, method="GET")
        with urllib.request.urlopen(req, timeout=30, context=ssl.create_default_context()) as response:
            verified = f"verified TLS succeeded ({response.status})"
    except urllib.error.HTTPError as err:
        verified = f"verified TLS succeeded ({err.code})"
    except (ssl.SSLError, urllib.error.URLError) as err:
        verified = f"verified TLS failed: {getattr(err, 'reason', err)}"
    try:
        cert = certificate(parsed.hostname, parsed.port or 443)
    except (OSError, ssl.SSLError) as err:
        cert = f"certificate unreadable: {type(err).__name__}: {err}"
    return (f"endpoint {label}", "INFO", f"{verified}; unverified -> {status}; {cert}; " + facts[0])


def probe_endpoint():
    rows = [endpoint_row("SPLUNK_INTEROP_HEC_URL", HEC_URL)]
    if HEC_URL_ALT:
        rows.append(endpoint_row("SPLUNK_INTEROP_HEC_URL_ALT", HEC_URL_ALT))
    return rows


def sized_event(marker, size):
    """One `/event` object of `size` bytes: `marker`, then padding."""
    skeleton = len(json.dumps({"event": marker + " "}))
    return json.dumps({"event": marker + " " + "x" * (size - skeleton)}).encode()


def probe_body_cap():
    cases = []
    for size in (999_000, 1_000_001, 1_048_577, 2_000_000):
        m = f"{RUN}-cap{size}"
        cases.append((f"{size:,} bytes uncompressed", m, sized_event(m, size), {}))
    m = f"{RUN}-capgzr"
    noise = base64.b64encode(os.urandom(1_500_000)).decode()
    body = json.dumps({"event": f"{m} {noise}"}).encode()[:2_000_000 - 2] + b'"}'
    cases.append(("2,000,000 bytes incompressible, gzip", m, gzip.compress(body), {"Content-Encoding": "gzip"}))
    m = f"{RUN}-capgzc"
    cases.append(("2,000,000 bytes compressible, gzip", m, gzip.compress(sized_event(m, 2_000_000)),
                  {"Content-Encoding": "gzip"}))
    rows, replies = [], []
    for label, marker, payload, headers in cases:
        try:
            status, reply, text = hec_full("/services/collector/event", payload, headers=headers)
            answer = (f"{status} {text[:200]!r}; Content-Type={reply.get('Content-Type')!r} "
                      f"Retry-After={reply.get('Retry-After')!r}")
        # A receiver that refuses a body mid-upload can close the connection before the client
        # reads its reply.
        except OSError as err:
            answer = f"no reply: {type(err).__name__}: {getattr(err, 'reason', err)}"
        replies.append((label, marker, len(payload), answer))
    found = indexed([marker for _, marker, *_ in replies], wait=60)
    for label, marker, sent_bytes, answer in replies:
        seen = found if found == NOT_SEARCHED else ("indexed" if marker in found else "not indexed")
        rows.append((f"body cap, {label}", "INFO", f"{sent_bytes:,} bytes on the wire -> {answer}; {seen}"))
    return rows


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
        seen = found if found == NOT_SEARCHED else [f.rsplit("-", 1)[1] for f in found]
        rows.append((f"batch with {label}", "INFO", f"{status} {text}; indexed {seen}"))
    # Whether a code 6 after an indexed prefix carries an `ackId` on a `useACK` token.
    if ACK_TOKEN:
        headers = {"X-Splunk-Request-Channel": str(uuid.uuid4())}
        for label, index in (("syntax error in object 1", 0), ("syntax error in object 0", 1)):
            m = f"{RUN}-ca{index}"
            status, text = hec("/services/collector/event", cases[index][1](m), token=ACK_TOKEN,
                               headers=headers)
            rows.append((f"useACK batch with {label}", "INFO", f"{status} {text}"))
    return rows


def probe_health_token():
    rows = []
    for path in ("/services/collector/health", "/services/collector/health/1.0"):
        answers = []
        for label, token in (("no token", None), ("the token", TOKEN), ("a bogus token", str(uuid.uuid4()))):
            headers = {"Authorization": f"Splunk {token}"} if token else {}
            status, _, body = request("GET", HEC + path, None, headers)
            answers.append(f"{label} -> {status} {body[:120]!r}")
        rows.append((f"GET {path}", "INFO", "; ".join(answers)))
    return rows


#: `event` values that carry nothing: (as written in the row, the value).
EMPTY_EVENTS = [("{}", {}), ("[]", []), ('" "', " "), ("null", None), ("0", 0), ("false", False)]


def probe_empty_event_object():
    m = f"{RUN}-empty"
    replies = []
    for i, (label, value) in enumerate(EMPTY_EVENTS):
        # The marker rides in `source`: the `event` has no room for one.
        body = objects({"event": value, "source": f"{m}{i}"})
        replies.append((label, f"{m}{i}", hec("/services/collector/event", body)))
    rows = try_search(f'search index=main source="{m}*" | table source _raw', lambda r: len(r) >= len(EMPTY_EVENTS))
    stored = {} if rows == NOT_SEARCHED else {r.get("source"): r.get("_raw") for r in rows}
    out = []
    for label, source, (status, text) in replies:
        if rows == NOT_SEARCHED:
            seen = rows
        else:
            seen = f"indexed, _raw {stored[source]!r}" if source in stored else "not indexed"
        out.append((f"`event: {label}`", "INFO", f"{status} {text}; {seen}"))
    return out


def probe_time_forms():
    m = f"{RUN}-tf"
    now = int(time.time()) - 60
    # Written as JSON text, so a float with nanosecond digits reaches Splunk as written.
    cases = [
        ("an integer in seconds", str(now)),
        ("an integer in milliseconds", f"{now}123"),
        ("an integer in nanoseconds", f"{now}123456789"),
        ("a decimal string", f'"{now}.123456789"'),
        ("a float with nanosecond digits", f"{now}.123456789"),
    ]
    replies = [(label, value, hec("/services/collector/event", f'{{"event": "{m}-{i}", "time": {value}}}'.encode()))
               for i, (label, value) in enumerate(cases)]
    rows = try_search(f'search index=main "{m}" | eval e=printf("%.9f", _time),'
                      ' s=strftime(_time, "%Y-%m-%dT%H:%M:%S.%9N") | table _raw e s',
                      lambda r: len(r) >= len(cases))
    stored = {} if rows == NOT_SEARCHED else {r.get("_raw"): r for r in rows}
    out = []
    for i, (label, value, (status, text)) in enumerate(replies):
        row = stored.get(f"{m}-{i}")
        if rows == NOT_SEARCHED:
            seen = rows
        else:
            seen = f"_time {row.get('e')} ({row.get('s')})" if row else "not indexed"
        out.append((f"`time` as {label}", "INFO", f"`time: {value}` -> {status} {text}; {seen}"))
    return out


def probe_envelope_carryover():
    m = f"{RUN}-env"
    then = int(time.time()) - 3600
    envelope = {"host": "probe-carry-host", "index": "osnix", "source": "probe-carry-source",
                "sourcetype": "probe:carry", "time": then}
    first = hec("/services/collector/event", objects({"event": f"{m}-first0", **envelope},
                                                     {"event": f"{m}-first1"}, {"event": f"{m}-first2"}))
    last = hec("/services/collector/event", objects({"event": f"{m}-last0"}, {"event": f"{m}-last1"},
                                                    {"event": f"{m}-last2", **envelope}))
    rows = try_search(f'search (index=main OR index=osnix) "{m}" | eval e=_time'
                      ' | table _raw index host source sourcetype e', lambda r: len(r) >= 6)
    stored = {} if rows == NOT_SEARCHED else {r.get("_raw"): r for r in rows}

    def landed(marker):
        row = stored.get(marker)
        if not row:
            return f"object {marker[-1]} not indexed"
        at = "the envelope's" if int(float(row.get("e") or 0)) == then else "not the envelope's"
        return (f"object {marker[-1]} -> index={row.get('index')} host={row.get('host')} "
                f"source={row.get('source')} sourcetype={row.get('sourcetype')} _time {at}")

    out = []
    for label, case, (status, text) in (("object 0", "first", first), ("object 2", "last", last)):
        seen = rows if rows == NOT_SEARCHED else "; ".join(landed(f"{m}-{case}{i}") for i in range(3))
        out.append((f"envelope on {label} of 3 only", "INFO", f"{status} {text}; {seen}"))
    return out


#: `/raw` bodies under a sourcetype with no props: (label, body from its marker).
RAW_BODIES = [
    ("three LF-terminated lines", lambda m: f"{m}-1 one\n{m}-2 two\n{m}-3 three\n"),
    ("three CRLF-terminated lines", lambda m: f"{m}-1 one\r\n{m}-2 two\r\n{m}-3 three\r\n"),
    ("three lines, no trailing newline", lambda m: f"{m}-1 one\n{m}-2 two\n{m}-3 three"),
    ("three lines, the second indented", lambda m: f"{m}-1 one\n    {m}-2 continued\n{m}-3 three\n"),
]


def probe_raw_merging():
    m = f"{RUN}-rm"
    replies = []
    for i, (label, body) in enumerate(RAW_BODIES):
        source = f"{m}{i}"
        query = urllib.parse.urlencode({"sourcetype": "logit:rawmerge", "source": source})
        replies.append((label, source, hec(f"/services/collector/raw?{query}", body(source).encode())))
    sources = {source for _, source, _ in replies}
    rows = try_search(f'search index=main source="{m}*" | table source _raw',
                      lambda r: {row.get("source") for row in r} >= sources)
    out = []
    for label, source, (status, text) in replies:
        if rows == NOT_SEARCHED:
            seen = rows
        else:
            raws = sorted(row.get("_raw", "") for row in rows if row.get("source") == source)
            seen = f"{len(raws)} event(s), _raw {raws}"
        out.append((f"/raw with {label}", "INFO", f"{status} {text}; {seen}"))
    return out


def probe_event_vs_raw_props():
    label = "`logit:ta` props on /raw and /event"
    if not LOCAL:
        return [(label, "SKIP", "cloud mode: the `logit:ta` props and transforms are the local stack's")]
    m = f"{RUN}-ta"
    stamp = int(time.time()) - 2 * 86400
    ts = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(stamp))
    line = lambda case: f"ts={ts} route=probe {m}-{case}"
    replies = [
        ("/raw?sourcetype=logit:ta", "raw",
         hec("/services/collector/raw?sourcetype=logit:ta", (line("raw") + "\n").encode())),
        ("/event, sourcetype logit:ta", "event",
         hec("/services/collector/event", objects({"event": line("event"), "sourcetype": "logit:ta"}))),
        ("/event?auto_extract_timestamp=true, sourcetype logit:ta", "auto",
         hec("/services/collector/event?auto_extract_timestamp=true",
             objects({"event": line("auto"), "sourcetype": "logit:ta"}))),
    ]
    rows = try_search(f'search (index=main OR index=tcpout_probe) "{m}" | eval e=_time'
                      ' | table _raw index sourcetype e', lambda r: len(r) >= len(replies), 60)
    parts = []
    for route, case, (status, text) in replies:
        if rows == NOT_SEARCHED:
            parts.append(f"{route} -> {status} {text}, {rows}")
            continue
        row = next((r for r in rows if r.get("_raw", "").endswith(f"{m}-{case}")), None)
        if row is None:
            parts.append(f"{route} -> {status} {text}, not indexed")
            continue
        at = "the line's ts" if int(float(row.get("e") or 0)) == stamp else "not the line's ts"
        parts.append(f"{route} -> {status}, index={row.get('index')} sourcetype={row.get('sourcetype')} "
                     f"_time {at}")
    return [(label, "INFO", "; ".join(parts))]


def probe_metrics():
    rows = []
    m = f"{RUN}-mt"
    mtype = hec("/services/collector/event", objects({"event": "metric", "index": "metrics",
                                                      "fields": {"probe": m, "metric_name:probe.mtype": 3,
                                                                 "metric_type": "Sum"}}))
    if index_refused(mtype[1]):
        return [("metrics index", "INFO", f"{mtype[0]} {mtype[1]}: the token may not write `metrics`; "
                                          "the metric probes need that index")]
    sc4s = hec("/services/collector/event", objects({"time": str(int(time.time())), "index": "metrics", "source": "sc4s",
                                                     "fields": {"probe": m, "metric_name:probe.sc4s": "5"}}))
    single = hec("/services/collector/event", objects({"event": "metric", "index": "metrics",
                                                       "fields": {"probe": m, "metric_name": "probe.single", "_value": 7}}))
    for n in (200, 1000):
        fields = {"probe": m, f"metric_name:probe.dims{n}": n, **{f"d{i}": f"v{i}" for i in range(n)}}
        status, text = hec("/services/collector/event", objects({"event": "metric", "index": "metrics", "fields": fields}))
        dims = try_search(f'| mcatalog values(_dims) as d where index=metrics metric_name="probe.dims{n}"',
                          lambda r: bool(r) and bool(r[0].get("d")))
        seen = (f"mcatalog {dims}" if dims == NOT_SEARCHED
                else f"mcatalog sees {len(as_list(dims[0].get('d'))) if dims else 0} dimensions")
        rows.append((f"metric event with {n} dimensions", "INFO", f"{status} {text}; {seen}"))
    stored = try_search('| mstats latest(_value) as v where index=metrics'
                        ' metric_name IN ("probe.mtype", "probe.sc4s", "probe.single") by metric_name',
                        lambda r: len(r) >= 3)
    by_type = try_search('| mstats latest(_value) as v where index=metrics metric_name="probe.mtype" by metric_type')
    pairs = lambda rows, key: rows if rows == NOT_SEARCHED else [(r.get(key), r.get("v")) for r in rows]
    rows.append(("metric_type", "INFO", f"-> {mtype[0]}; mstats by metric_type {pairs(by_type, 'metric_type')}"))
    rows.append(("metric object forms", "INFO",
                 f"no `event` + string value -> {sc4s[0]}; single-metric form -> {single[0]}; stored "
                 f"{pairs(stored, 'metric_name')}"))
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
    # A load balancer in front of HEC can pin an ack channel to one indexer with a cookie.
    status, headers, _ = hec_full("/services/collector/event", objects({"event": f"{RUN}-cookie"}))
    cookies = [c.split("=", 1)[0].strip() + "=<redacted>" for c in headers.get_all("Set-Cookie") or []]
    rows.append(("Set-Cookie on an /event reply", "INFO", f"{status}; {cookies or 'none'}"))
    return rows


def ack_poll(channel, ids):
    """One `/ack` poll on the ack token: (status, reply text, {id: bool})."""
    status, text = hec("/services/collector/ack", json.dumps({"acks": ids}).encode(), token=ACK_TOKEN,
                       headers={"X-Splunk-Request-Channel": channel})
    try:
        acks = json.loads(text).get("acks", {})
    except (ValueError, AttributeError):
        acks = {}
    return status, text, acks


def poll_until(channel, ids, want, timeout=60):
    """Polls `ids` on `channel` until every id in `want` has answered `true` once, or `timeout`:
    (the ids that ever answered `true`, the last reply, seconds taken). Splunk may forget an id
    once it has answered `true`, so the ids are collected across polls."""
    seen, started = set(), time.monotonic()
    while True:
        _, text, acks = ack_poll(channel, ids)
        seen |= {int(k) for k, v in acks.items() if v is True}
        took = time.monotonic() - started
        if want <= seen or took > timeout:
            return sorted(seen), text, took
        time.sleep(0.5)


def probe_ack_ids():
    if not ACK_TOKEN:
        return [("useACK answers", "SKIP", "no SPLUNK_INTEROP_ACK_TOKEN")]
    a, b = str(uuid.uuid4()), str(uuid.uuid4())

    def post(marker, channel):
        headers = {"X-Splunk-Request-Channel": channel} if channel else None
        return hec("/services/collector/event", objects({"event": f"{RUN}-{marker}"}), token=ACK_TOKEN,
                   headers=headers)

    no_channel = post("ack-none", None)
    a0, a1, b0 = post("ack-a0", a)[1], post("ack-a1", a)[1], post("ack-b0", b)[1]
    a_true, a_last, a_took = poll_until(a, [0, 1, 5], {0, 1})
    _, a_again, _ = ack_poll(a, [0, 1])
    b_true, b_last, _ = poll_until(b, [0, 1], {0}, timeout=30)
    _, fresh, _ = ack_poll(str(uuid.uuid4()), [0])
    return [
        ("useACK without a channel", "INFO", f"{no_channel[0]} {no_channel[1]}"),
        ("useACK ids", "INFO", f"channel A's two posts -> {a0}, {a1}; channel B's one -> {b0}"),
        ("useACK polls", "INFO",
         f"A polling [0, 1, 5]: ids ever true {a_true} within {a_took:.1f}s, last reply {a_last}; "
         f"A polling [0, 1] again after both were true -> {a_again}; B polling [0, 1]: ids ever true "
         f"{b_true}, last reply {b_last}; a new channel polling [0] -> {fresh}"),
    ]


def probe_tcpout():
    if not LOCAL:
        return [("[tcpout] sendCookedData=false", "SKIP", "cloud mode: no `rawcap` receiver or output group")]
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


PROBES = [probe_version, probe_endpoint, probe_health_token, probe_body_cap, probe_gzip, probe_code6,
          probe_empty_event_object, probe_time_forms, probe_envelope_carryover, probe_raw_merging,
          probe_event_vs_raw_props, probe_metrics, probe_http, probe_ack_ids, probe_tcpout]


# ---- main ---------------------------------------------------------------------------------------


def write_spl():
    since = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(int(SINCE)))
    lines = [f"# SPL for a manual search pass over this run, whose events were indexed from {SINCE} ({since}).",
             f"# Run each over all time, adding _index_earliest={SINCE} to a `search` query to leave out",
             "# earlier runs: the relay leg's events carry their recorded timestamps, so an event-time",
             "# range misses them."]
    label = None
    for owner, query in SPL:
        if owner != label:
            lines += ["", f"# {owner}"]
            label = owner
        lines.append(query)
    (OUT / "search.spl").write_text(scrub("\n".join(lines) + "\n"))


def main():
    results = []
    for leg, title, check, service, queries in LEGS:
        CURRENT[0] = leg
        for query in queries:
            record_spl(query)
        try:
            result, detail = check()
        except SearchUnavailable:
            result, detail = sent(leg, service, queries)
        except Exception as err:  # A query that failed is a failed leg, not a crashed harness.
            result, detail = "FAIL", f"{type(err).__name__}: {err}"
        results.append({"leg": leg, "title": title, "result": result, "detail": detail})

    probe_rows = []
    for probe in PROBES:
        CURRENT[0] = probe.__name__
        try:
            probe_rows += probe()
        except Exception as err:
            probe_rows.append((probe.__name__, "FAIL", f"{type(err).__name__}: {err}"))

    cell = lambda text: str(text).replace("|", "\\|").replace("\n", "\\n")
    lines = ["| Leg | What | Result | Detail |", "|---|---|---|---|"]
    lines += [f"| {r['leg']} | {r['title']} | {r['result']} | {cell(r['detail'])} |" for r in results]
    lines += ["", "| Probe | Result | Detail |", "|---|---|---|"]
    lines += [f"| {probe} | {result} | {cell(detail)} |" for probe, result, detail in probe_rows]
    table = scrub("\n".join(lines))
    print(table)
    (OUT / "results.md").write_text(table + "\n")
    (OUT / "results.json").write_text(scrub(json.dumps(
        {"target": TARGET, "search": SEARCH, "since": SINCE, "legs": results,
         "probes": [dict(zip(("probe", "result", "detail"), r)) for r in probe_rows]},
        indent=2)) + "\n")
    write_spl()
    failed = any(r["result"] == "FAIL" for r in results) or any(r[1] == "FAIL" for r in probe_rows)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
