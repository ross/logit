#!/usr/bin/env python3
"""Prunes a `script/record-fixtures datadog-agent` capture, then checks it holds what the tests read.

The Agent posts host metadata to `/intake/` alongside its events. That body identifies the
recording machine (hardware and disk UUIDs, CPU model, kernel build), so this deletes every
`/intake/` body with no `events` key before anything else, and fails the run if any kept body still
carries one of `HOST_MARKERS`. `script/record-fixtures` runs it whether or not the capture completed,
because the capture writes straight into `testdata/`.

The Agent flushes on its own clock (series, sketches, service checks, and events every 15 s; stats
every 10 s), so which request carries the DogStatsD clients' data depends on when they sent. The
capture keeps several requests per route to cover that; this check makes the recipe guarantee the
result instead of a reviewer eyeballing it. It fails the run when any construct below is missing
from every captured request on its route.

Needs `zstandard`, which `script/record-fixtures` `pip install`s; everything else is stdlib. Bodies
are decompressed and searched for names; only an `/intake/` body is parsed, for its top-level keys.
"""

import gzip
import json
import pathlib
import sys
import zlib

import zstandard

#: route file prefix -> byte strings that must appear in at least one of its decompressed bodies.
EXPECTED = {
    "agent-api-v2-series-": [
        b"record.requests.count",
        b"record.queue.depth",
        b"record.batch.lag",
        b"record.users.active",
        b"record.request.duration.avg",
        b"record.db.query.duration.95percentile",
    ],
    "agent-api-beta-sketches-": [b"record.response.size"],
    "agent-api-v1-check-run-": [b"record.can_connect", b"slow upstream"],
    "agent-intake-": [b"Deploy finished", b"Agent Startup"],
    "agent-api-v2-logs-": [b"slow upstream"],
    "agent-api-v0-2-traces-": [b"flask.request", b"orders.lookup"],
    "agent-api-v0-2-stats-": [b"flask.request"],
}

#: Strings that mark a body as describing the recording machine rather than the producers' data.
#: Searched case-insensitively in every kept body, decompressed.
HOST_MARKERS = [b"/dev/", b'"uuid"', b"cpu_cores", b"model_name", b"kernel_version", b"luks"]


def remove(body_path):
    body_path.unlink()
    body_path.with_suffix(".headers").unlink(missing_ok=True)
    print(f"check_datadog_agent_capture: removed {body_path.name}: not kept")


def prune(out_dir):
    """Deletes host metadata: `/api/v2/host_metadata` and every `/intake/` body with no events."""
    for body_path in sorted(out_dir.glob("agent-api-v2-host-metadata-*.bin")):
        remove(body_path)
    for body_path in sorted(out_dir.glob("agent-intake-*.bin")):
        body = json.loads(decompressed(body_path))
        if not isinstance(body, dict) or "events" not in body:
            remove(body_path)


def decompressed(body_path):
    body = body_path.read_bytes()
    headers = body_path.with_suffix(".headers").read_text().splitlines()
    encoding = next((h.split(": ", 1)[1] for h in headers if h.startswith("content-encoding: ")), "")
    if encoding == "zstd":
        return zstandard.ZstdDecompressor().decompressobj().decompress(body)
    if encoding == "gzip":
        return gzip.decompress(body)
    if encoding == "deflate":
        return zlib.decompress(body)
    return body


def main():
    out_dir = pathlib.Path(sys.argv[1])
    prune(out_dir)
    exposed = []
    for body_path in sorted(out_dir.glob("agent-*.bin")):
        body = decompressed(body_path).lower()
        hits = [m.decode() for m in HOST_MARKERS if m in body]
        if hits:
            exposed.append(f"{body_path.name}: {', '.join(hits)}")
    if exposed:
        print("check_datadog_agent_capture: kept bodies describe the recording machine:")
        for line in exposed:
            print("  " + line)
        print("Don't commit them: delete the files and find the route that let them through.")
        sys.exit(1)

    missing = []
    for prefix, needles in EXPECTED.items():
        bodies = [decompressed(p) for p in sorted(out_dir.glob(prefix + "*.bin"))]
        if not bodies:
            missing.append(f"{prefix}*: no request captured")
            continue
        for needle in needles:
            if not any(needle in body for body in bodies):
                missing.append(f"{prefix}*: none of {len(bodies)} request(s) carries {needle!r}")
    if missing:
        print("check_datadog_agent_capture: the capture is missing constructs the tests read:")
        for line in missing:
            print("  " + line)
        print("Re-record: the Agent's flush landed outside the captured requests.")
        sys.exit(1)
    print("check_datadog_agent_capture: every expected construct is in the capture")


if __name__ == "__main__":
    main()
