#!/usr/bin/env python3
"""Replays every request recorded in testdata/interop/splunk/ at the `hec-relay` leg's listener.

The `replay` service in compose.yaml runs this once `logit-hec-relay` is healthy. Each capture
goes as recorded: its method, its path with the query string, every recorded header but the ones
the HTTP client sets itself, and its body as sent (still gzip where it was). One line per request
lands in /out/replay.log, `<status> <method> <path> <file>`, which check.py reads.

Stdlib only. Usage: replay.py <capture dir> <base URL>
"""

import pathlib
import sys
import urllib.error
import urllib.request

#: Headers the client computes or that describe the recorded connection, not the request.
SKIP = {"host", "content-length", "connection", "accept-encoding", "transfer-encoding", "expect"}


def main():
    captures, base = pathlib.Path(sys.argv[1]), sys.argv[2].rstrip("/")
    lines = []
    for body_path in sorted(captures.glob("*.bin")):
        method, path, headers = "POST", "/", {}
        for line in body_path.with_suffix(".headers").read_text().splitlines():
            name, _, value = line.partition(": ")
            if name == "method":
                method = value
            elif name == "path":
                path = value
            elif name not in SKIP:
                headers[name] = value
        body = body_path.read_bytes() if method in ("POST", "PUT") else None
        request = urllib.request.Request(base + path, data=body, method=method, headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                status = response.status
        except urllib.error.HTTPError as err:
            status = err.code
        except OSError as err:
            status = f"error:{type(err).__name__}"
        lines.append(f"{status} {method} {path.split('?', 1)[0]} {body_path.name}")
    pathlib.Path("/out/replay.log").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
