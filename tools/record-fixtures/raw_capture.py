#!/usr/bin/env python3
"""Raw-capture sink for `script/record-fixtures`.

Binds a UDP, TCP, or HTTP listener and writes each arrival to its own file verbatim, with no
parsing, validation, or re-encoding, so a fixture is exactly what the producer put on the wire.
Runs in a throwaway `python:3.12-slim` container on the producer's Docker network, never on the
host.

Usage:
    raw_capture.py --proto udp  --port 5514 --out-dir /out --prefix logger --count 2 --timeout 15
    raw_capture.py --proto tcp  --port 601  --out-dir /out --prefix rsyslog --count 1 --timeout 20
    raw_capture.py --proto http --port 9091 --out-dir /out --prefix prometheus-v1 --count 2

UDP: one file per datagram (`<prefix>-000.raw`, `<prefix>-001.raw`, ...).

TCP: one file per accepted connection, holding everything read until the peer closes it or
--timeout passes with no new bytes, so framing (RFC 6587) is kept as sent.

HTTP: per request, the body byte-for-byte as `<prefix>-000.bin`, plus a `<prefix>-000.headers`
sidecar with `method:`, `path:`, and every header (name lowercased, in received order), so a
replay test can read the content type, encoding, and version headers. This mode answers
`204 No Content`, because an HTTP client won't send a second request to a listener that never
replied. It refuses a `POST` with no `Content-Length`, or with any `Transfer-Encoding`, with
`411 Length Required`, not counted toward --count: de-framing a chunked body is re-encoding.
Connections are served on threads, so one idle peer can't park the capture. Nothing here is
remote-write-specific.

Exits 0 only if --count datagrams, connections, or requests arrive before --timeout; a partial
capture exits 1 so `script/record-fixtures` fails. For udp/tcp, --timeout bounds the wait for each
message; for http it bounds the whole capture, since threads accept an idle connection at once.
"""

import argparse
import http.server
import pathlib
import socket
import socketserver
import sys
import threading
import time


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


#: How often the HTTP accept loop wakes to check whether its handler threads finished the capture.
POLL_SECONDS = 0.25


def capture_http(port: int, out_dir: pathlib.Path, prefix: str, count: int, timeout: float) -> int:
    # Stdlib only: this runs in a bare `python:3.12-slim` with no `pip install` step.
    # `claimed` counts requests that took a sequence number, `done` responses flushed to the
    # socket. The accept loop watches `done`: stopping when the last body hits disk would let
    # `server_close()` close the socket under a handler still writing its `204`, which the sender
    # sees as a failure.
    state = {"claimed": 0, "done": 0, "spare": 0}
    seq_lock = threading.Lock()
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

        # BaseHTTPRequestHandler dispatches on "do_" + the method, so the name is fixed. Other
        # methods get the base class's 501.
        def do_POST(self) -> None:
            # Refuse a body this mode can't capture verbatim rather than record it empty: chunked
            # transfer needs de-framing, which is re-encoding, and without a valid Content-Length
            # there's no way to know where the body ends. Otherwise each would land a 0-byte
            # `.bin`, answer `204`, and count; an unparseable length would raise in `int()` and
            # drop the connection with no status. The 411 isn't counted, so a run against such a
            # producer times out and exits 1.
            why = None
            length = 0
            encoding = self.headers.get("Transfer-Encoding")
            length_header = self.headers.get("Content-Length")
            if encoding:
                why = f"Transfer-Encoding: {encoding}"
            elif length_header is None:
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

            # Claim the sequence number under the lock, so two concurrent senders can't both be
            # `-000`. A request after `count` is claimed is answered but not written: an extra
            # file would need deleting at review, and a mid-write teardown could truncate it.
            with seq_lock:
                if state["claimed"] >= count:
                    spare = True
                    index = 0
                else:
                    spare = False
                    index = state["claimed"]
                    state["claimed"] = index + 1
            if spare:
                # Counted, not printed: this runs on a daemon thread the main thread may already be
                # past, and writing to stdout there races interpreter shutdown
                # (CPython aborts with "could not acquire lock for <stdout> at interpreter
                # shutdown, possibly due to daemon threads"). The main thread reports the total.
                with seq_lock:
                    state["spare"] += 1
                self.send_response(204)
                self.end_headers()
                self.close_connection = True
                return
            # `.bin`, not `.raw`: this is an HTTP entity body (for remote-write, Snappy-compressed
            # protobuf), and the request framing lives in the sidecar.
            body_path = out_dir / f"{prefix}-{index:03d}.bin"
            body_path.write_bytes(body)
            # Lowercased, because `self.headers` keeps the sender's case and a replay test
            # shouldn't depend on it.
            headers_path = out_dir / f"{prefix}-{index:03d}.headers"
            lines = [f"method: {self.command}", f"path: {self.path}"]
            lines += [f"{name.lower()}: {value}" for name, value in self.headers.items()]
            headers_path.write_text("\n".join(lines) + "\n")

            print(
                f"raw_capture: {len(body)} bytes from {self.client_address} -> {body_path}"
                f" (+{headers_path.name})",
                flush=True,
            )

            # A remote-write receiver's success answer, which lets the sender send the next request.
            self.send_response(204)
            self.end_headers()
            self.wfile.flush()
            with seq_lock:
                state["done"] += 1
                enough = state["done"] >= count
            if enough:
                # Enough captured: let the handler's keep-alive loop return so the accept loop
                # below can stop, instead of sitting on this connection until it times out.
                self.close_connection = True

        def log_message(self, fmt: str, *args) -> None:
            # Route the base class's stderr request log through print(), so finish_capture's log
            # dump reads as one stream. Silent once the capture is complete: a stdout write from a
            # daemon thread racing interpreter shutdown aborts the process.
            with seq_lock:
                if state["done"] >= count:
                    return
            print(f"raw_capture: {fmt % args}", flush=True)

    # Threaded, unlike the UDP/TCP modes: a kept-alive connection can sit idle, and serving
    # connections one at a time would let one idle peer (a probe, a shard with nothing to flush)
    # park the capture. `daemon_threads` so a handler on an idle connection can't keep the process
    # alive once `count` is in.
    class Server(socketserver.ThreadingTCPServer):
        allow_reuse_address = True
        daemon_threads = True

    server = Server(("0.0.0.0", port), Handler, bind_and_activate=False)
    # Bounds one `handle_request()` call -- how long the *accept* waits, not the capture.
    server.timeout = timeout
    server.server_bind()
    server.server_activate()
    print(f"raw_capture: listening http/{port}, want {count} request(s)", flush=True)

    # One deadline for the whole capture: with threads an idle connection is accepted at once, so
    # an accept that produced nothing doesn't mean nothing is coming. --timeout is how long the run
    # has to produce `count` requests, over any number of connections.
    deadline = time.monotonic() + timeout
    try:
        while state["done"] < count:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                print(
                    f"raw_capture: timed out after {state['done']}/{count} requests",
                    file=sys.stderr,
                )
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
    # a sender that never got its `204` failed a capture that looks fine here.
    time.sleep(0.2)
    with seq_lock:
        spare = state["spare"]
    if spare:
        print(
            f"raw_capture: {spare} further request(s) arrived after {count} were captured,"
            " answered and discarded",
            flush=True,
        )
    return state["done"]


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
