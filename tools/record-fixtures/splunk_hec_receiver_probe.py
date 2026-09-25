#!/usr/bin/env python3
"""Posts the recorded `splunk_hec` exporter span body to the Collector's own `splunk_hec` receiver.

`script/record-fixtures splunk-otel` runs this after its capture, against the same Collector,
whose `logs/hec_receiver` pipeline prints what the receiver made of the body through the `debug`
exporter. It settles whether that receiver decodes the exporter's span objects back to spans
(`docs/plans/splunk-relay.md`, "Unverified, to be settled by W5", item 6). The body goes as
recorded, with its `Content-Encoding`, so the receiver sees the exporter's bytes.

Usage: splunk_hec_receiver_probe.py <capture dir> <receiver URL>
"""

import gzip
import pathlib
import sys
import urllib.error
import urllib.request


def encoding(body_path):
    for line in body_path.with_suffix(".headers").read_text().splitlines():
        if line.startswith("content-encoding: "):
            return line.split(": ", 1)[1]
    return ""


def main():
    out_dir, url = pathlib.Path(sys.argv[1]), sys.argv[2]
    for body_path in sorted(out_dir.glob("otel-services-collector-*.bin")):
        body = body_path.read_bytes()
        plain = gzip.decompress(body) if encoding(body_path) == "gzip" else body
        if b'"parent_span_id"' not in plain:
            continue
        headers = {"Authorization": "Splunk 00000000-0000-0000-0000-000000000000"}
        if encoding(body_path):
            headers["Content-Encoding"] = encoding(body_path)
        request = urllib.request.Request(url, data=body, method="POST", headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                status, reply = response.status, response.read()
        except urllib.error.HTTPError as err:
            status, reply = err.code, err.read()
        print(f"splunk_hec_receiver_probe: {body_path.name} -> {status} {reply.decode(errors='replace')}")
        return
    print("splunk_hec_receiver_probe: no span body in the capture", file=sys.stderr)
    sys.exit(1)


if __name__ == "__main__":
    main()
