#!/usr/bin/env python3
"""Replay recorded wire captures at a live `logit` listener, for `script/shape-survey`.

The mirror image of `tools/record-fixtures/raw_capture.py`: it sends recorded bytes back verbatim,
with no parsing or re-encoding, because anything reinterpreted here would show up as a property the
producer never had. Stdlib only; runs in a throwaway `python:3.12-slim` on the producer's network.

Usage:
    replay.py --proto udp  --host logit --port 8125 --files '/corpus/statsd/*.raw'
    replay.py --proto tcp  --host logit --port 601  --files '/corpus/syslog/rsyslog-tcp-*.raw'
    replay.py --proto http --url http://logit:9201/api/v1/write --files '/corpus/prometheus/*.bin'
    replay.py --proto http --url http://logit:4318/v1/logs --json-lines --files '/corpus/otlp/logs.json'

udp: one datagram per file, in sorted filename order. `--pps` paces the send (default 200): a
dropped datagram is a silently wrong measurement, not a visible failure.

tcp: one connection per file, the unit `raw_capture.py --proto tcp` recorded, so each capture's
own framing (RFC 6587 non-transparent or octet-counted, a carbon pickle length prefix) replays as
the producer framed it.

http: one POST per file. A `<name>.headers` sidecar next to a `<name>.bin` supplies that request's
`content-type`, `content-encoding`, and protocol version header, so a remote-write body replays as
the version it was. `--json-lines` instead POSTs each line of a `.json`/`.jsonl` file as
`application/json`, the shape the Collector's `file` exporter writes.

Exits 0 only if every send succeeded and every HTTP response was 2xx, so a partial replay fails
the survey rather than summarizing a capture missing half its traffic.
"""

import argparse
import glob
import http.client
import pathlib
import socket
import sys
import time
import urllib.parse

#: Sidecar headers that describe the body and must be replayed with it. The rest are regenerated
#: by this client or don't affect decoding.
REPLAYED_HEADERS = (
    "content-type",
    "content-encoding",
    "x-prometheus-remote-write-version",
)


def files_for(patterns: list[str]) -> list[pathlib.Path]:
    """Every file matching the glob patterns, de-duplicated, in sorted order.

    Sorted, not directory order: the recorded corpora are numbered (`-000`, `-001`, ...) and a
    producer's packing decisions read in that order. Fails on an empty match rather than replaying
    nothing successfully -- a typo'd path is otherwise indistinguishable from a clean run.
    """
    found: list[pathlib.Path] = []
    seen: set[str] = set()
    for pattern in patterns:
        for name in sorted(glob.glob(pattern)):
            path = pathlib.Path(name)
            if path.is_file() and name not in seen:
                seen.add(name)
                found.append(path)
    if not found:
        print(f"replay: no files matched {patterns}", file=sys.stderr)
        sys.exit(1)
    return found


def pace(pps: float) -> None:
    if pps > 0:
        time.sleep(1.0 / pps)


def replay_udp(paths: list[pathlib.Path], host: str, port: int, pps: float, repeat: int) -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sent = 0
    for _ in range(repeat):
        for path in paths:
            data = path.read_bytes()
            sock.sendto(data, (host, port))
            sent += 1
            print(f"replay: udp {len(data)} bytes <- {path.name}", flush=True)
            pace(pps)
    sock.close()
    return sent


def replay_tcp(paths: list[pathlib.Path], host: str, port: int, pps: float, repeat: int) -> int:
    sent = 0
    for _ in range(repeat):
        for path in paths:
            data = path.read_bytes()
            with socket.create_connection((host, port), timeout=10) as conn:
                conn.sendall(data)
                # Half-close rather than a bare close: an LF-framed stream listener flushes its
                # last partial line on EOF, and shutting down the write side sends that EOF while
                # the peer finishes reading.
                conn.shutdown(socket.SHUT_WR)
            sent += 1
            print(f"replay: tcp {len(data)} bytes <- {path.name}", flush=True)
            pace(pps)
    return sent


def sidecar_headers(path: pathlib.Path) -> dict[str, str]:
    """The body-describing headers from `<stem>.headers`, if one exists.

    Parsed with the same trivial `name: value` grammar `raw_capture.py` writes (names already
    lowercased there), and filtered to REPLAYED_HEADERS -- see that tuple's own comment.
    """
    sidecar = path.with_suffix(".headers")
    headers: dict[str, str] = {}
    if not sidecar.is_file():
        return headers
    for line in sidecar.read_text().splitlines():
        name, _, value = line.partition(":")
        name = name.strip().lower()
        if name in REPLAYED_HEADERS:
            headers[name] = value.strip()
    return headers


def post(url_parts: urllib.parse.ParseResult, body: bytes, headers: dict[str, str]) -> None:
    """One POST, raising on anything but a 2xx.

    A fresh connection per request rather than one kept alive across the corpus: these are a
    handful of requests, and a connection per request means one failure cannot be confused with a
    stale keep-alive being closed under us.
    """
    conn = http.client.HTTPConnection(url_parts.hostname, url_parts.port or 80, timeout=30)
    try:
        path = url_parts.path or "/"
        conn.request("POST", path, body=body, headers={**headers, "Content-Length": str(len(body))})
        response = conn.getresponse()
        payload = response.read()
        if not 200 <= response.status < 300:
            raise RuntimeError(f"POST {path} -> {response.status} {response.reason}: {payload[:200]!r}")
    finally:
        conn.close()


def replay_http(
    paths: list[pathlib.Path], url: str, json_lines: bool, pps: float, repeat: int
) -> int:
    parts = urllib.parse.urlparse(url)
    sent = 0
    for _ in range(repeat):
        for path in paths:
            if json_lines:
                # The Collector's `file` exporter writes one JSON object per line; each line is
                # its own complete OTLP/JSON request body.
                for line in path.read_text().splitlines():
                    if not line.strip():
                        continue
                    post(parts, line.encode("utf-8"), {"Content-Type": "application/json"})
                    sent += 1
                    print(f"replay: http {len(line)} bytes <- {path.name}", flush=True)
                    pace(pps)
            else:
                body = path.read_bytes()
                headers = sidecar_headers(path)
                headers.setdefault("content-type", "application/x-protobuf")
                post(parts, body, headers)
                sent += 1
                print(f"replay: http {len(body)} bytes <- {path.name} ({headers})", flush=True)
                pace(pps)
    return sent


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--proto", choices=["udp", "tcp", "http"], required=True)
    ap.add_argument("--host", help="udp/tcp target host")
    ap.add_argument("--port", type=int, help="udp/tcp target port")
    ap.add_argument("--url", help="http target URL, e.g. http://logit:9201/api/v1/write")
    ap.add_argument(
        "--files", nargs="+", required=True, help="one or more globs of files to replay, in order"
    )
    ap.add_argument(
        "--pps",
        type=float,
        default=200.0,
        help="sends per second (0 disables pacing). Modest by default: a shape survey must not"
        " drop datagrams, and a drop is a silently wrong measurement",
    )
    ap.add_argument("--repeat", type=int, default=1, help="loop the corpus N times")
    ap.add_argument(
        "--json-lines",
        action="store_true",
        help="http only: POST each LINE of each file as application/json (OTLP/JSON captures)",
    )
    args = ap.parse_args()

    if args.proto in ("udp", "tcp") and (not args.host or not args.port):
        ap.error("--host and --port are required for --proto udp/tcp")
    if args.proto == "http" and not args.url:
        ap.error("--url is required for --proto http")

    paths = files_for(args.files)
    try:
        if args.proto == "udp":
            sent = replay_udp(paths, args.host, args.port, args.pps, args.repeat)
        elif args.proto == "tcp":
            sent = replay_tcp(paths, args.host, args.port, args.pps, args.repeat)
        else:
            sent = replay_http(paths, args.url, args.json_lines, args.pps, args.repeat)
    except Exception as err:  # noqa: BLE001 -- any send failure is a failed replay, reported as one
        print(f"replay: failed -- {err}", file=sys.stderr)
        sys.exit(1)

    print(f"replay: sent {sent} message(s) from {len(paths)} file(s)", flush=True)


if __name__ == "__main__":
    main()
