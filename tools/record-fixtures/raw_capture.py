#!/usr/bin/env python3
"""Generic raw-capture sink for `script/record-fixtures`.

Binds a UDP or TCP listener and writes each thing that arrives to its own file, verbatim --
**no parsing, no validation, no re-encoding.** That's the whole point of a recorded fixture: it
has to be exactly what a real producer put on the wire, so `logit`'s decoder is checked against
that producer's actual behavior instead of against this script's understanding of the same spec.

Runs inside a throwaway `python:3-slim` (or similar) container on the same Docker network as the
producer container being recorded (`script/record-fixtures` wires up both) -- never against a host
Python, per `docs/adr/containerized-development.md`.

Usage:
    raw_capture.py --proto udp --port 5514 --out-dir /out --prefix logger --count 2 --timeout 15
    raw_capture.py --proto tcp --port 601   --out-dir /out --prefix rsyslog --count 1 --timeout 20

UDP: one file per datagram (`<prefix>-000.raw`, `<prefix>-001.raw`, ...) -- syslog/UDP has no
framing beyond "one datagram is one message", so this is the natural unit.

TCP: one file per accepted connection, containing everything read until the peer closes it (or
--timeout elapses with no new bytes) -- TCP syslog framing (octet-counting vs. non-transparent
trailer, RFC 6587) is exactly one of the things a recorded fixture should capture *as sent*, not
normalize away here.

HTTP is not implemented -- nothing this corpus records yet needs it (OTLP fixtures are captured
via the Collector's own `file` exporter instead, see `tools/record-fixtures/otel-collector-config.yaml`
and the plan doc's "How captures are recorded" section). If a future producer needs a raw HTTP
capture sink, add a `capture_http` alongside `capture_udp`/`capture_tcp` below, following the same
shape: bind, accept up to --count requests or --timeout elapses, write each request body verbatim.

Exit status is 0 only if --count messages/connections were captured before --timeout; a partial
capture exits 1 so `script/record-fixtures` can fail loudly instead of silently committing an
empty or truncated fixture set.
"""

import argparse
import pathlib
import socket
import sys


def capture_udp(port: int, out_dir: pathlib.Path, prefix: str, count: int, timeout: float) -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", port))
    sock.settimeout(timeout)
    print(f"raw_capture: listening udp/{port}, want {count} datagram(s)", flush=True)

    got = 0
    while got < count:
        try:
            data, addr = sock.recvfrom(65536)
        except socket.timeout:
            print(f"raw_capture: timed out after {got}/{count} datagrams", file=sys.stderr)
            break
        path = out_dir / f"{prefix}-{got:03d}.raw"
        path.write_bytes(data)
        print(f"raw_capture: {len(data)} bytes from {addr} -> {path}", flush=True)
        got += 1
    return got


def capture_tcp(port: int, out_dir: pathlib.Path, prefix: str, count: int, timeout: float) -> int:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("0.0.0.0", port))
    listener.listen(1)
    listener.settimeout(timeout)
    print(f"raw_capture: listening tcp/{port}, want {count} connection(s)", flush=True)

    got = 0
    while got < count:
        try:
            conn, addr = listener.accept()
        except socket.timeout:
            print(f"raw_capture: timed out after {got}/{count} connections", file=sys.stderr)
            break
        conn.settimeout(timeout)
        chunks = []
        try:
            while True:
                chunk = conn.recv(65536)
                if not chunk:
                    break
                chunks.append(chunk)
        except socket.timeout:
            pass  # peer went quiet without closing -- keep what arrived, same as a real capture.
        finally:
            conn.close()
        data = b"".join(chunks)
        path = out_dir / f"{prefix}-{got:03d}.raw"
        path.write_bytes(data)
        print(f"raw_capture: {len(data)} bytes from {addr} -> {path}", flush=True)
        got += 1
    return got


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--proto", choices=["udp", "tcp"], required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--out-dir", required=True, help="Directory to write captured fixtures into (must exist)")
    ap.add_argument("--prefix", required=True, help="Filename prefix, e.g. 'logger' -> logger-000.raw")
    ap.add_argument("--count", type=int, default=1, help="Number of datagrams/connections to capture")
    ap.add_argument("--timeout", type=float, default=10.0, help="Seconds to wait for each message before giving up")
    args = ap.parse_args()

    out_dir = pathlib.Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    capture = capture_udp if args.proto == "udp" else capture_tcp
    got = capture(args.port, out_dir, args.prefix, args.count, args.timeout)

    if got != args.count:
        print(f"raw_capture: only captured {got}/{args.count} -- failing so a partial fixture set isn't committed", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
