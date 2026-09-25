#!/usr/bin/env python3
"""Raw-capture sink for `script/record-fixtures`.

Binds a UDP, TCP, Unix datagram, Unix stream, or HTTP listener and writes each arrival to its own
file verbatim, with no parsing, validation, or re-encoding, so a fixture is exactly what the
producer put on the wire. Runs in a throwaway `python:3.12-slim` container on the producer's Docker
network, never on the host.

Usage:
    raw_capture.py --proto udp  --port 5514 --out-dir /out --prefix logger --count 2 --timeout 15
    raw_capture.py --proto tcp  --port 601  --out-dir /out --prefix rsyslog --count 1 --timeout 20
    raw_capture.py --proto unix --path /var/run/datadog/dsd.socket --out-dir /out --prefix dsd --count 9
    raw_capture.py --proto unix-stream --path /var/run/datadog/dsd-stream.socket --out-dir /out --prefix dsd-stream
    raw_capture.py --proto http --port 9091 --out-dir /out --prefix prometheus-v1 --count 2
    raw_capture.py --proto http --port 8126 --out-dir /out --prefix tracer --name-by-path \
        --reply /info=/replies/info.json --require /v0.4/traces --require /v0.6/stats

UDP and Unix datagram: one file per datagram (`<prefix>-000.raw`, `<prefix>-001.raw`, ...).

TCP and Unix stream: one file per accepted connection, holding everything read until the peer
closes it or --timeout passes with no new bytes, so framing (RFC 6587, a length prefix) is kept as
sent.

The Unix modes bind --path, replacing a stale socket file, and make it mode 0777 so a client
running as any user can send; a real client connects to a path, not a port, so these are how a
socket-only framing reaches disk.

HTTP: per request, the body byte-for-byte as `<prefix>-000.bin`, plus a `<prefix>-000.headers`
sidecar with `method:`, `path:`, and every header (name lowercased, in received order), so a
replay test can read the content type, encoding, and version headers. `GET`, `POST`, and `PUT` are
captured; a `GET` has an empty `.bin`. It refuses a `POST` or `PUT` with no `Content-Length`, or
with any `Transfer-Encoding`, with `411 Length Required`, not counted: de-framing a chunked body is
re-encoding. Connections are served on threads, so one idle peer can't park the capture.

The HTTP reply is `--status` (default `204 No Content`) with an empty body, because an HTTP client
won't send a second request to a listener that never replied. `--reply PATH=FILE` answers requests
for PATH (the query string ignored) with `200` and FILE's bytes as `application/json` instead, for
a client that reads the answer: a dd-trace tracer reads `GET /info` to choose its trace form, and a
trace `POST`'s reply for its sampling rates.

HTTP completion is one of two rules:
- Without `--require`: --count requests, over any paths.
- With `--name-by-path`: each request is named after its path (`<prefix>-<slug>-000.bin`,
  `/api/v2/series` -> `api-v2-series`), numbered per path, and at most --max-per-path are written
  per path (the rest are answered and discarded). `--require PATH[=N]` makes the capture complete
  once N requests (default 1) to each required PATH are written, and raises that path's own limit
  to N. A sender with many routes, such as
  a Datadog Agent, is captured whole without one chatty route crowding out the rest.

`--discard PATH` answers every request for PATH and writes nothing, whichever rule applies: for a
route whose body is never wanted on disk, such as an Agent's host metadata.

Exits 0 only if the capture completes before --timeout; a partial capture exits 1 so
`script/record-fixtures` fails. For udp/tcp/unix/unix-stream, --timeout bounds the wait for each
message; for http it bounds the whole capture, since threads accept an idle connection at once.
"""

import argparse
import http.server
import os
import pathlib
import re
import socket
import socketserver
import sys
import threading
import time


def bind_unix(kind: int, path: str) -> socket.socket:
    """A Unix socket of `kind` bound at `path`, replacing a stale socket file an earlier run left."""
    if os.path.exists(path):
        os.unlink(path)
    sock = socket.socket(socket.AF_UNIX, kind)
    sock.bind(path)
    os.chmod(path, 0o777)
    return sock


def capture_datagrams(sock: socket.socket, what: str, out_dir: pathlib.Path, prefix: str, count: int, timeout: float) -> int:
    sock.settimeout(timeout)
    print(f"raw_capture: listening {what}, want {count} datagram(s)", flush=True)

    got = 0
    while got < count:
        try:
            data, addr = sock.recvfrom(65536)
        except socket.timeout:
            print(f"raw_capture: timed out after {got}/{count} datagrams", file=sys.stderr)
            break
        path = out_dir / f"{prefix}-{got:03d}.raw"
        path.write_bytes(data)
        print(f"raw_capture: {len(data)} bytes from {addr or what} -> {path}", flush=True)
        got += 1
    return got


def capture_streams(listener: socket.socket, what: str, out_dir: pathlib.Path, prefix: str, count: int, timeout: float) -> int:
    listener.listen(1)
    listener.settimeout(timeout)
    print(f"raw_capture: listening {what}, want {count} connection(s)", flush=True)

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
        print(f"raw_capture: {len(data)} bytes from {addr or what} -> {path}", flush=True)
        got += 1
    return got


def capture_udp(args, out_dir: pathlib.Path) -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", args.port))
    return capture_datagrams(sock, f"udp/{args.port}", out_dir, args.prefix, args.count, args.timeout)


def capture_tcp(args, out_dir: pathlib.Path) -> int:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("0.0.0.0", args.port))
    return capture_streams(listener, f"tcp/{args.port}", out_dir, args.prefix, args.count, args.timeout)


def capture_unix(args, out_dir: pathlib.Path) -> int:
    sock = bind_unix(socket.SOCK_DGRAM, args.path)
    return capture_datagrams(sock, f"unix:{args.path}", out_dir, args.prefix, args.count, args.timeout)


def capture_unix_stream(args, out_dir: pathlib.Path) -> int:
    listener = bind_unix(socket.SOCK_STREAM, args.path)
    return capture_streams(listener, f"unix-stream:{args.path}", out_dir, args.prefix, args.count, args.timeout)


#: How often the HTTP accept loop wakes to check whether its handler threads finished the capture.
POLL_SECONDS = 0.25


def path_slug(path: str) -> str:
    """A filename-safe name for a request path: `/api/v0.2/traces` -> `api-v0-2-traces`."""
    return re.sub(r"[^A-Za-z0-9]+", "-", path).strip("-") or "root"


class Plan:
    """What an HTTP capture writes, and when it is complete (the module doc's two rules)."""

    def __init__(self, args):
        self.by_path = args.name_by_path
        self.count = args.count
        self.max_per_path = args.max_per_path
        self.discard = set(args.discard)
        self.require = {}
        for spec in args.require:
            path, _, n = spec.partition("=")
            self.require[path] = int(n) if n else 1
        self.written = {}  # path -> requests written
        self.total = 0  # requests claimed, either rule
        self.done = 0  # responses flushed, the no-`--require` rule's measure

    def claim(self, path: str):
        """The file stem for a request to `path`, or None when it is answered and discarded."""
        if path in self.discard:
            return None
        if not self.by_path:
            if self.total >= self.count:
                return None
            stem = f"{self.total:03d}"
            self.total += 1
            return stem
        n = self.written.get(path, 0)
        if n >= max(self.max_per_path, self.require.get(path, 0)):
            return None
        self.written[path] = n + 1
        self.total += 1
        return f"{path_slug(path)}-{n:03d}"

    def complete(self) -> bool:
        if self.require:
            return all(self.written.get(p, 0) >= n for p, n in self.require.items())
        return self.done >= self.count

    def missing(self) -> str:
        if self.require:
            return ", ".join(
                f"{p} {self.written.get(p, 0)}/{n}" for p, n in self.require.items() if self.written.get(p, 0) < n
            )
        return f"{self.done}/{self.count} requests"


def capture_http(args, out_dir: pathlib.Path) -> int:
    # Stdlib only: this runs in a bare `python:3.12-slim` with no `pip install` step.
    # The accept loop watches `plan.complete()`, which under the `--count` rule counts responses
    # flushed to the socket: stopping when the last body hits disk would let `server_close()` close
    # the socket under a handler still writing its reply, which the sender sees as a failure.
    prefix, timeout = args.prefix, args.timeout
    plan = Plan(args)
    state = {"spare": 0}
    seq_lock = threading.Lock()
    replies = {}
    for spec in args.reply:
        path, _, file = spec.partition("=")
        replies[path] = pathlib.Path(file).read_bytes()
    # Bound under a second name because `timeout = timeout` inside the class body below would be
    # read as the class attribute being defined, not as this function's argument.
    conn_timeout = timeout

    class Handler(http.server.BaseHTTPRequestHandler):
        # HTTP/1.1 for keep-alive: a remote-write sender reuses one connection, and announcing
        # HTTP/1.0 would force a reconnect per request that the producer wouldn't otherwise make.
        protocol_version = "HTTP/1.1"
        # Applied by StreamRequestHandler.setup() as the connection's socket timeout, so a
        # kept-alive connection that goes quiet is dropped instead of blocking the accept loop.
        timeout = conn_timeout

        # BaseHTTPRequestHandler dispatches on "do_" + the method, so the names are fixed. Other
        # methods get the base class's 501.
        def do_GET(self) -> None:
            self.capture(has_body=False)

        def do_POST(self) -> None:
            self.capture(has_body=True)

        def do_PUT(self) -> None:
            self.capture(has_body=True)

        def respond(self, path: str) -> None:
            body = replies.get(path)
            if body is None:
                self.send_response(args.status)
                self.send_header("Content-Length", "0")
                self.end_headers()
            else:
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            self.wfile.flush()

        def capture(self, has_body: bool) -> None:
            # Refuse a body this mode can't capture verbatim rather than record it empty: chunked
            # transfer needs de-framing, which is re-encoding, and without a valid Content-Length
            # there's no way to know where the body ends. Otherwise each would land a 0-byte
            # `.bin`, answer, and count; an unparseable length would raise in `int()` and drop the
            # connection with no status. The 411 isn't counted, so a run against such a producer
            # times out and exits 1.
            why = None
            length = 0
            encoding = self.headers.get("Transfer-Encoding")
            length_header = self.headers.get("Content-Length")
            if encoding:
                why = f"Transfer-Encoding: {encoding}"
            elif length_header is None:
                if has_body:
                    why = "no Content-Length"
            else:
                try:
                    length = int(length_header)
                except ValueError:
                    why = f"unparseable Content-Length: {length_header!r}"
                else:
                    if length < 0:
                        why = f"negative Content-Length: {length_header!r}"
            if why is not None:
                print(
                    f"raw_capture: refusing a request from {self.client_address} -- {why};"
                    " this mode records a body verbatim or not at all",
                    file=sys.stderr,
                    flush=True,
                )
                self.send_response(411)
                self.send_header("Content-Length", "0")
                self.end_headers()
                # The body was never read, so whatever is still in flight would be parsed as the
                # next request line on a kept-alive connection. Close instead.
                self.close_connection = True
                return
            # Content-Length bytes, written with no decoding or decompression.
            body = self.rfile.read(length) if length else b""
            path = self.path.split("?", 1)[0]

            # Claim the file name under the lock, so two concurrent senders can't both be `-000`.
            # A request past the plan is answered but not written: an extra file would need
            # deleting at review, and a mid-write teardown could truncate it.
            with seq_lock:
                stem = None if plan.complete() else plan.claim(path)
            if stem is None:
                # Counted, not printed: this runs on a daemon thread the main thread may already be
                # past, and writing to stdout there races interpreter shutdown
                # (CPython aborts with "could not acquire lock for <stdout> at interpreter
                # shutdown, possibly due to daemon threads"). The main thread reports the total.
                with seq_lock:
                    state["spare"] += 1
                self.respond(path)
                return
            # `.bin`, not `.raw`: this is an HTTP entity body (for remote-write, Snappy-compressed
            # protobuf), and the request framing lives in the sidecar.
            body_path = out_dir / f"{prefix}-{stem}.bin"
            body_path.write_bytes(body)
            # Lowercased, because `self.headers` keeps the sender's case and a replay test
            # shouldn't depend on it.
            headers_path = out_dir / f"{prefix}-{stem}.headers"
            lines = [f"method: {self.command}", f"path: {self.path}"]
            lines += [f"{name.lower()}: {value}" for name, value in self.headers.items()]
            headers_path.write_text("\n".join(lines) + "\n")

            print(
                f"raw_capture: {self.command} {self.path}: {len(body)} bytes from"
                f" {self.client_address} -> {body_path} (+{headers_path.name})",
                flush=True,
            )

            self.respond(path)
            with seq_lock:
                plan.done += 1
                enough = plan.complete()
            if enough:
                # Enough captured: let the handler's keep-alive loop return so the accept loop
                # below can stop, instead of sitting on this connection until it times out.
                self.close_connection = True

        def log_message(self, fmt: str, *args) -> None:
            # Route the base class's stderr request log through print(), so finish_capture's log
            # dump reads as one stream. Silent once the capture is complete: a stdout write from a
            # daemon thread racing interpreter shutdown aborts the process.
            with seq_lock:
                if plan.complete():
                    return
            print(f"raw_capture: {fmt % args}", flush=True)

    # Threaded, unlike the UDP/TCP modes: a kept-alive connection can sit idle, and serving
    # connections one at a time would let one idle peer (a probe, a shard with nothing to flush)
    # park the capture. `daemon_threads` so a handler on an idle connection can't keep the process
    # alive once the capture is complete.
    class Server(socketserver.ThreadingTCPServer):
        allow_reuse_address = True
        daemon_threads = True

    server = Server(("0.0.0.0", args.port), Handler, bind_and_activate=False)
    # Bounds one `handle_request()` call -- how long the *accept* waits, not the capture.
    server.timeout = timeout
    server.server_bind()
    server.server_activate()
    want = ", ".join(f"{p}={n}" for p, n in plan.require.items()) or f"{plan.count} request(s)"
    print(f"raw_capture: listening http/{args.port}, want {want}", flush=True)

    # One deadline for the whole capture: with threads an idle connection is accepted at once, so
    # an accept that produced nothing doesn't mean nothing is coming. --timeout is how long the run
    # has to complete, over any number of connections.
    deadline = time.monotonic() + timeout
    try:
        while True:
            with seq_lock:
                if plan.complete():
                    break
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                with seq_lock:
                    missing = plan.missing()
                print(f"raw_capture: timed out; still missing {missing}", file=sys.stderr)
                break
            # A short slice, not the whole remaining budget: `handle_request()` returns only on an
            # accept or its own timeout, not when a handler thread finishes. A producer that sends
            # every request down one kept-alive connection and goes quiet would otherwise hold the
            # loop until the deadline with the capture already complete.
            server.timeout = min(POLL_SECONDS, remaining)
            server.handle_request()
    finally:
        server.server_close()
    # Let a handler that was mid-response finish: `server_close()` doesn't join daemon threads, and
    # a sender that never got its reply failed a capture that looks fine here.
    time.sleep(0.2)
    with seq_lock:
        spare = state["spare"]
        complete = plan.complete()
        total = plan.total
    if spare:
        print(
            f"raw_capture: {spare} further request(s) arrived past what the capture keeps,"
            " answered and discarded",
            flush=True,
        )
    print(f"raw_capture: wrote {total} request(s)", flush=True)
    return complete


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--proto", choices=["udp", "tcp", "unix", "unix-stream", "http"], required=True)
    ap.add_argument("--port", type=int, default=0, help="Port for udp/tcp/http")
    ap.add_argument("--path", help="Socket path for unix/unix-stream")
    ap.add_argument("--out-dir", required=True, help="Directory to write captured fixtures into (must exist)")
    ap.add_argument("--prefix", required=True, help="Filename prefix, 'logger' -> logger-000.raw (-000.bin/.headers for http)")
    ap.add_argument("--count", type=int, default=1, help="Number of datagrams/connections/requests to capture")
    ap.add_argument("--timeout", type=float, default=10.0, help="Seconds to wait for each message before giving up")
    ap.add_argument("--status", type=int, default=204, help="http: the status answered where no --reply applies")
    ap.add_argument("--reply", action="append", default=[], metavar="PATH=FILE", help="http: answer PATH with 200 and FILE as JSON")
    ap.add_argument("--name-by-path", action="store_true", help="http: name and number files per request path")
    ap.add_argument("--max-per-path", type=int, default=1, help="http, with --name-by-path: requests written per path")
    ap.add_argument("--discard", action="append", default=[], metavar="PATH", help="http: answer PATH but never write it")
    ap.add_argument("--require", action="append", default=[], metavar="PATH[=N]", help="http, with --name-by-path: complete once N requests to PATH are written")
    args = ap.parse_args()

    if args.proto in ("unix", "unix-stream") and not args.path:
        ap.error(f"--proto {args.proto} needs --path")
    if args.proto in ("udp", "tcp", "http") and not args.port:
        ap.error(f"--proto {args.proto} needs --port")
    if args.require and not args.name_by_path:
        ap.error("--require needs --name-by-path")

    out_dir = pathlib.Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.proto == "http":
        if not capture_http(args, out_dir):
            print("raw_capture: capture incomplete -- failing so a partial fixture set isn't committed", file=sys.stderr)
            sys.exit(1)
        return

    capture = {"udp": capture_udp, "tcp": capture_tcp, "unix": capture_unix, "unix-stream": capture_unix_stream}[args.proto]
    got = capture(args, out_dir)
    if got != args.count:
        print(f"raw_capture: only captured {got}/{args.count} -- failing so a partial fixture set isn't committed", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
