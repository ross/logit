#!/usr/bin/env python3
"""Generic raw-capture sink for `script/record-fixtures`.

Binds a UDP, TCP or HTTP listener and writes each thing that arrives to its own file, verbatim --
**no parsing, no validation, no re-encoding.** That's the whole point of a recorded fixture: it
has to be exactly what a real producer put on the wire, so `logit`'s decoder is checked against
that producer's actual behavior instead of against this script's understanding of the same spec.

Runs inside a throwaway `python:3-slim` (or similar) container on the same Docker network as the
producer container being recorded (`script/record-fixtures` wires up both) -- never against a host
Python, per `docs/adr/containerized-development.md`.

Usage:
    raw_capture.py --proto udp  --port 5514 --out-dir /out --prefix logger --count 2 --timeout 15
    raw_capture.py --proto tcp  --port 601  --out-dir /out --prefix rsyslog --count 1 --timeout 20
    raw_capture.py --proto http --port 9091 --out-dir /out --prefix prometheus-v1 --count 2

UDP: one file per datagram (`<prefix>-000.raw`, `<prefix>-001.raw`, ...) -- syslog/UDP has no
framing beyond "one datagram is one message", so this is the natural unit.

TCP: one file per accepted connection, containing everything read until the peer closes it (or
--timeout elapses with no new bytes) -- TCP syslog framing (octet-counting vs. non-transparent
trailer, RFC 6587) is exactly one of the things a recorded fixture should capture *as sent*, not
normalize away here.

HTTP: two files per accepted request -- the request **body** byte-for-byte as
`<prefix>-000.bin`, plus a `<prefix>-000.headers` sidecar holding `method:`, `path:` and every
request header (name lowercased, one per line, in the order received). The body is written with no
decoding, no decompression and no re-encoding, exactly like the two modes above; the sidecar exists
because an HTTP producer's framing lives in its headers rather than in the bytes on the wire, so a
replay test can read the content type, content encoding and any version header off the capture
instead of guessing them. Unlike `capture_tcp`, this mode **answers** -- `204 No Content`, the
status a remote-write receiver returns for a successful write. Answering is not a nicety: a real
HTTP client will not send a second request to a listener that never replied to the first, which is
exactly why `capture_tcp`, which only ever reads, cannot record a multi-request HTTP exchange.
Nothing here is remote-write-specific -- it is a plain "record what was POSTed" sink, reusable by
any future HTTP-shaped fixture work. (OTLP is the one HTTP-ish corpus that does *not* use it: those
fixtures come from the Collector's own `file` exporter instead, see
`tools/record-fixtures/otel-collector-config.yaml` and the plan doc's "How captures are recorded"
section.)

Exit status is 0 only if --count messages/connections/requests were captured before --timeout; a
partial capture exits 1 so `script/record-fixtures` can fail loudly instead of silently committing
an empty or truncated fixture set.
"""

import argparse
import http.server
import pathlib
import socket
import socketserver
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


def capture_http(port: int, out_dir: pathlib.Path, prefix: str, count: int, timeout: float) -> int:
    # Stdlib only, on purpose: this runs in a bare `python:3.12-slim` with no `pip install` step,
    # same as the UDP/TCP modes (see this module's docstring and `script/record-fixtures`).
    state = {"got": 0}
    # Bound under a second name because `timeout = timeout` inside the class body below would be
    # read as the class attribute being defined, not as this function's argument.
    conn_timeout = timeout

    class Handler(http.server.BaseHTTPRequestHandler):
        # HTTP/1.1 so keep-alive works: a real remote-write sender reuses one connection for
        # several requests, and a listener that announced HTTP/1.0 would force a reconnect per
        # request -- a capture artifact of this script rather than the producer's own behavior.
        protocol_version = "HTTP/1.1"
        # Applied by StreamRequestHandler.setup() as the connection's socket timeout, so a
        # kept-alive connection that goes quiet is dropped instead of blocking the accept loop.
        timeout = conn_timeout

        # `do_POST`, not `do_post`: BaseHTTPRequestHandler dispatches on "do_" + the request
        # method verbatim, so the name is the framework's, not a style choice. Anything that
        # isn't a POST gets the base class's own 501, which is the honest answer from a sink that
        # only knows how to record request bodies.
        def do_POST(self) -> None:
            # Exactly Content-Length bytes, straight to disk: no decoding, no decompression, no
            # re-encoding, so the fixture is the producer's bytes and nothing else. A body with no
            # Content-Length (chunked) isn't supported -- no producer this corpus records sends
            # one, and guessing at a re-assembly would defeat the point of a verbatim capture.
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length) if length else b""

            index = state["got"]
            # `.bin`, not the `.raw` the UDP/TCP corpora use: what lands here is an HTTP entity
            # body (for the first consumer, a Snappy-compressed protobuf blob), not a raw wire
            # capture in the same sense -- the framing that made it a request lives in the sidecar.
            body_path = out_dir / f"{prefix}-{index:03d}.bin"
            body_path.write_bytes(body)
            # Header names lowercased by hand: `self.headers` preserves whatever case the sender
            # used, and a replay test reading `content-type` shouldn't have to care which case a
            # given Prometheus build happened to send.
            headers_path = out_dir / f"{prefix}-{index:03d}.headers"
            lines = [f"method: {self.command}", f"path: {self.path}"]
            lines += [f"{name.lower()}: {value}" for name, value in self.headers.items()]
            headers_path.write_text("\n".join(lines) + "\n")

            state["got"] = index + 1
            print(
                f"raw_capture: {len(body)} bytes from {self.client_address} -> {body_path}"
                f" (+{headers_path.name})",
                flush=True,
            )

            # 204 No Content -- what a remote-write receiver answers a successful write, and what
            # makes the sender willing to send the next request at all (see the module docstring).
            self.send_response(204)
            self.end_headers()
            if state["got"] >= count:
                # Enough captured: let the handler's keep-alive loop return so the accept loop
                # below can stop, instead of sitting on this connection until it times out.
                self.close_connection = True

        def log_message(self, fmt: str, *args) -> None:
            # BaseHTTPRequestHandler logs every request straight to stderr in its own format;
            # route it through the same print() the other two modes use so finish_capture's log
            # dump reads as one stream.
            print(f"raw_capture: {fmt % args}", flush=True)

    server = socketserver.TCPServer(("0.0.0.0", port), Handler, bind_and_activate=False)
    server.allow_reuse_address = True
    server.timeout = timeout
    server.server_bind()
    server.server_activate()
    print(f"raw_capture: listening http/{port}, want {count} request(s)", flush=True)

    try:
        while state["got"] < count:
            before = state["got"]
            # One accepted connection per call, which may carry several keep-alive requests;
            # returns without handling anything once --timeout elapses with nobody connecting.
            server.handle_request()
            if state["got"] == before:
                print(
                    f"raw_capture: timed out after {state['got']}/{count} requests",
                    file=sys.stderr,
                )
                break
    finally:
        server.server_close()
    return state["got"]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--proto", choices=["udp", "tcp", "http"], required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--out-dir", required=True, help="Directory to write captured fixtures into (must exist)")
    ap.add_argument("--prefix", required=True, help="Filename prefix, 'logger' -> logger-000.raw (-000.bin/.headers for http)")
    ap.add_argument("--count", type=int, default=1, help="Number of datagrams/connections/requests to capture")
    ap.add_argument("--timeout", type=float, default=10.0, help="Seconds to wait for each message before giving up")
    args = ap.parse_args()

    out_dir = pathlib.Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    capture = {"udp": capture_udp, "tcp": capture_tcp, "http": capture_http}[args.proto]
    got = capture(args.port, out_dir, args.prefix, args.count, args.timeout)

    if got != args.count:
        print(f"raw_capture: only captured {got}/{args.count} -- failing so a partial fixture set isn't committed", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
