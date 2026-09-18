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
A request this mode cannot record verbatim is **refused, not recorded empty**: a `POST` with no
`Content-Length`, or any `Transfer-Encoding`, gets `411 Length Required` and does not count toward
--count, because de-framing a chunked body is the re-encoding a recorded fixture exists to avoid
and there is no other way to know where such a body ends. Connections are served on threads and
--count requests may arrive over any number of them, so one idle peer cannot park the capture.
Nothing here is remote-write-specific -- it is a plain "record what was POSTed" sink, reusable by
any future HTTP-shaped fixture work. (OTLP is the one HTTP-ish corpus that does *not* use it: those
fixtures come from the Collector's own `file` exporter instead, see
`tools/record-fixtures/otel-collector-config.yaml` and the plan doc's "How captures are recorded"
section.)

Exit status is 0 only if --count messages/connections/requests were captured before --timeout; a
partial capture exits 1 so `script/record-fixtures` can fail loudly instead of silently committing
an empty or truncated fixture set. For udp/tcp --timeout bounds the wait for each message; for http
it bounds the **whole capture**, since with threads an idle connection is accepted at once and
forever, and "nothing arrived on this accept" stops being a signal that nothing is coming.
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


def capture_http(port: int, out_dir: pathlib.Path, prefix: str, count: int, timeout: float) -> int:
    # Stdlib only, on purpose: this runs in a bare `python:3.12-slim` with no `pip install` step,
    # same as the UDP/TCP modes (see this module's docstring and `script/record-fixtures`).
    # `claimed` is bumped when a request takes its sequence number, `done` when its response has
    # been flushed onto the socket. The accept loop below watches `done`, not `claimed`: handlers
    # run on their own threads now, so exiting the loop the instant the last body hit disk would
    # let `server_close()` tear the socket down under a handler still writing its `204` -- which
    # is a capture the *sender* sees fail.
    state = {"claimed": 0, "done": 0, "spare": 0}
    seq_lock = threading.Lock()
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
            # A body this mode cannot capture verbatim is refused, never recorded empty. Chunked
            # transfer would have to be de-framed to be written out, and de-framing is exactly the
            # re-encoding a recorded fixture exists to avoid; an absent Content-Length leaves no
            # way to know where the body ends. Both would otherwise land a 0-byte `.bin`, answer
            # `204` and count toward --count -- the silent empty-fixture outcome this module's
            # docstring and `finish_capture` both promise cannot happen. `411 Length Required` is
            # the status HTTP has for exactly this, and the request is *not* counted, so a run
            # against such a producer times out and exits 1 rather than committing nothing.
            length_header = self.headers.get("Content-Length")
            encoding = self.headers.get("Transfer-Encoding")
            if length_header is None or encoding:
                why = f"Transfer-Encoding: {encoding}" if encoding else "no Content-Length"
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
            # Exactly Content-Length bytes, straight to disk: no decoding, no decompression, no
            # re-encoding, so the fixture is the producer's bytes and nothing else.
            length = int(length_header)
            body = self.rfile.read(length) if length else b""

            # Handlers run on their own threads, so the sequence number is claimed under the lock:
            # two senders writing at once must not both be `-000`. A request that arrives once
            # `count` is already claimed records nothing at all -- it is still answered, so the
            # sender sees a clean exchange, but writing an N+1'th fixture into the output directory
            # would leave a file `script/record-fixtures`'s own review step has to notice and
            # delete (and, if this server is torn down mid-write, a truncated one).
            with seq_lock:
                if state["claimed"] >= count:
                    spare = True
                    index = 0
                else:
                    spare = False
                    index = state["claimed"]
                    state["claimed"] = index + 1
            if spare:
                # Counted, not printed. This runs on a daemon thread that the main thread is
                # already on its way past, and writing to stdout there races interpreter shutdown
                # (CPython aborts with "could not acquire lock for <stdout> at interpreter
                # shutdown, possibly due to daemon threads"). The main thread reports the total.
                with seq_lock:
                    state["spare"] += 1
                self.send_response(204)
                self.end_headers()
                self.close_connection = True
                return
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

            print(
                f"raw_capture: {len(body)} bytes from {self.client_address} -> {body_path}"
                f" (+{headers_path.name})",
                flush=True,
            )

            # 204 No Content -- what a remote-write receiver answers a successful write, and what
            # makes the sender willing to send the next request at all (see the module docstring).
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
            # BaseHTTPRequestHandler logs every request straight to stderr in its own format;
            # route it through the same print() the other two modes use so finish_capture's log
            # dump reads as one stream. Silent once the capture is complete, for the reason the
            # discard path above gives: these run on daemon threads, and a write to stdout racing
            # interpreter shutdown aborts the process.
            with seq_lock:
                if state["done"] >= count:
                    return
            print(f"raw_capture: {fmt % args}", flush=True)

    # Threaded, unlike the UDP/TCP modes' single accept loop: a connection here can be kept alive
    # with nothing on it, and serving those one at a time means one idle peer (a health check, a
    # load balancer's probe, a second Prometheus shard that has nothing to flush yet) parks the
    # whole capture while the request being waited for queues behind it. `daemon_threads` so a
    # handler still sitting on an idle connection cannot keep the process alive once `count` is in.
    class Server(socketserver.ThreadingTCPServer):
        allow_reuse_address = True
        daemon_threads = True

    server = Server(("0.0.0.0", port), Handler, bind_and_activate=False)
    # Bounds one `handle_request()` call -- how long the *accept* waits, not the capture.
    server.timeout = timeout
    server.server_bind()
    server.server_activate()
    print(f"raw_capture: listening http/{port}, want {count} request(s)", flush=True)

    # One deadline for the whole capture, rather than "--timeout with nothing accepted". With
    # threads, an idle connection is accepted immediately and forever, so the old "this
    # `handle_request` produced nothing, give up" test would fire on the first probe even while a
    # real sender was mid-handshake. The deadline is what --timeout means for this mode: the run
    # has this long to produce `count` requests, however many connections it takes.
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
            server.timeout = remaining
            server.handle_request()
    finally:
        server.server_close()
    # A moment for any handler that was mid-response when the count was reached to finish writing
    # it: `server_close()` does not join daemon threads, and a sender that never got its `204` is
    # a capture that looks fine here and failed at the other end.
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
