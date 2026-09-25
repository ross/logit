#!/usr/bin/env python3
"""Checks a `script/record-fixtures splunk` capture for the recording machine and for coverage.

`script/record-fixtures` runs this after the four Splunk producers, whatever their outcome,
because the capture writes straight into `testdata/`. Every `.bin` is decompressed per its
`.headers` sidecar's `Content-Encoding` and searched, with its sidecar, for anything describing
the machine that recorded it:

- the recording machine's hostname, which the script reads at run time and passes in;
- `check_datadog_agent_capture.py`'s `HOST_MARKERS`;
- an IPv4 address outside loopback and Docker's `172.16.0.0/12`, other than telemetrygen's fixed
  `network.peer.address` (`TELEMETRYGEN_PEER`).

It then fails the run unless each construct `crates/logit-proto/tests/splunk_interop.rs` reads
appears in at least one capture (`EXPECTED`).

Stdlib only. Usage: check_splunk_capture.py <capture dir> <recording hostname>
"""

import gzip
import ipaddress
import json
import pathlib
import re
import sys

#: Strings that mark a body as describing the recording machine (the Datadog check's list).
HOST_MARKERS = [b"/dev/", b'"uuid"', b"cpu_cores", b"model_name", b"kernel_version", b"luks"]

#: telemetrygen writes this peer address on every span; it names no machine.
TELEMETRYGEN_PEER = "1.2.3.4"

DOCKER_NET = ipaddress.ip_network("172.16.0.0/12")
IPV4 = re.compile(rb"(?<![\d.])(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})(?![\d.])")


def sidecar(body_path):
    lines = body_path.with_suffix(".headers").read_text().splitlines()
    return dict(line.split(": ", 1) for line in lines if ": " in line)


def decompressed(body_path):
    body = body_path.read_bytes()
    if sidecar(body_path).get("content-encoding") == "gzip":
        return gzip.decompress(body)
    return body


def objects(body):
    """The HEC objects of a `/event` body: concatenated objects or one array."""
    text = body.decode("utf-8", errors="replace").strip()
    decoder = json.JSONDecoder()
    found, i = [], 0
    while i < len(text):
        value, i = decoder.raw_decode(text, i)
        found.extend(value if isinstance(value, list) else [value])
        while i < len(text) and text[i].isspace():
            i += 1
    return found


def is_raw(body_path):
    return sidecar(body_path).get("path", "").startswith("/services/collector/raw")


def metric_keys(obj):
    fields = obj.get("fields") or {}
    return [k for k in fields if k.startswith("metric_name:")]


def is_span(event):
    return isinstance(event, dict) and all(k in event for k in ("trace_id", "span_id", "start_time", "end_time"))


#: construct -> a test over (body path, decoded objects or None for `/raw`).
EXPECTED = {
    "an `event` string": lambda p, objs: any(isinstance(o.get("event"), str) and o["event"] != "metric" for o in objs or []),
    "an `event` object": lambda p, objs: any(isinstance(o.get("event"), dict) and not is_span(o["event"]) for o in objs or []),
    "a metric event with one `metric_name:` field": lambda p, objs: any(o.get("event") == "metric" and len(metric_keys(o)) == 1 for o in objs or []),
    "a multi-metric event (several `metric_name:` fields)": lambda p, objs: any(o.get("event") == "metric" and len(metric_keys(o)) > 1 for o in objs or []),
    "a histogram `_bucket` with `le`": lambda p, objs: any("le" in (o.get("fields") or {}) and any(k.endswith("_bucket") for k in metric_keys(o)) for o in objs or []),
    "a span object (`trace_id`, `span_id`, `start_time`, `end_time`)": lambda p, objs: any(is_span(o.get("event")) for o in objs or []),
    "`fields` with `otel.log.severity.number`": lambda p, objs: any("otel.log.severity.number" in (o.get("fields") or {}) for o in objs or []),
    "a `/raw` body": lambda p, objs: objs is None and p.stat().st_size > 0,
    "`/raw` metadata in the query string": lambda p, objs: objs is None and "sourcetype=" in sidecar(p).get("path", ""),
    "a gzip `Content-Encoding`": lambda p, objs: sidecar(p).get("content-encoding") == "gzip",
    "Docker's `OPTIONS` connection check": lambda p, objs: sidecar(p).get("method") == "OPTIONS",
    "a health check `GET`": lambda p, objs: sidecar(p).get("method") == "GET" and "/health" in sidecar(p).get("path", ""),
    "SC4S's syslog-derived `sourcetype`": lambda p, objs: any(o.get("sourcetype") == "nix:syslog" for o in objs or []),
    "the Java appender's `severity` in the event": lambda p, objs: any(isinstance(o.get("event"), dict) and "severity" in o["event"] for o in objs or []),
}


def exposures(body_path, hostname):
    hits = []
    for label, data in (("body", decompressed(body_path)), ("headers", body_path.with_suffix(".headers").read_bytes())):
        lowered = data.lower()
        if hostname and hostname.lower().encode() in lowered:
            hits.append(f"{label}: the recording hostname")
        hits += [f"{label}: {m.decode()}" for m in HOST_MARKERS if m in lowered]
        for match in IPV4.findall(data):
            text = match.decode()
            try:
                address = ipaddress.ip_address(text)
            except ValueError:
                continue
            if address.is_loopback or address in DOCKER_NET or text == TELEMETRYGEN_PEER:
                continue
            hits.append(f"{label}: address {text}")
    return hits


def main():
    out_dir, hostname = pathlib.Path(sys.argv[1]), sys.argv[2] if len(sys.argv) > 2 else ""
    bodies = sorted(out_dir.glob("*.bin"))
    if not bodies:
        print("check_splunk_capture: no captures")
        sys.exit(1)

    exposed = []
    for body_path in bodies:
        exposed += [f"{body_path.name}: {hit}" for hit in exposures(body_path, hostname)]
    if exposed:
        print("check_splunk_capture: captures describe the recording machine:")
        for line in exposed:
            print("  " + line)
        print("Don't commit them: delete the files and find the producer setting that let them through.")
        sys.exit(1)

    parsed = []
    for body_path in bodies:
        body = decompressed(body_path)
        if is_raw(body_path) or not body.strip():
            parsed.append((body_path, None))
        else:
            parsed.append((body_path, objects(body)))
    missing = [name for name, test in EXPECTED.items() if not any(test(p, objs) for p, objs in parsed)]
    if missing:
        print("check_splunk_capture: the capture is missing constructs the tests read:")
        for name in missing:
            print("  " + name)
        sys.exit(1)
    print(f"check_splunk_capture: {len(bodies)} captures, nothing describes the recording machine, every expected construct present")


if __name__ == "__main__":
    main()
