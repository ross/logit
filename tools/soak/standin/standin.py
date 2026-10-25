"""A scripted HTTP destination for a soak's SUT sink: it answers each write as the driver last
told it to, and prints one JSON line per request to stdout, the record the checks read.

Stdlib only; compose runs it in a stock Python image. It serves two ports:

- The data port (`--data-port`) takes the sink's writes on `--path`. With no cue it answers the
  shape's default with an empty body: `200` on `/api/v1/write` for `remote_write`, `202` on
  `/api/v2/series` for `datadog`, and `204` on `/api/v2/write` for `influxdb`, with no
  `Content-Length`, which a `204` mustn't carry. The path is compared without its query string,
  which `influxdb_out` always sends (`org`, `orgID`, `bucket`, `precision`). A request to any
  other path answers `404`, so a misconfigured endpoint can't pass for a stored write.
- The control port (`--control-port`) takes `POST /cue` from the driver: a JSON object with
  `step`, and either `status` (with an optional `body`) or `answer: "reset"`, and an optional
  `apply` and `count`. With `count`, N, the cue answers the next N writes on `--path` and is then
  spent: the stand-in answers the default again, recording no cue, until the next `POST /cue`.
  With several requests in flight, that lands a cue on one of them. Under the `datadog` shape a `2xx` status can carry `series_errors`, N: the answer's
  body is then the series route's `errors` list, protobuf field 1 (`repeated string`), with N
  entries, which `datadog_out` counts as N series the intake dropped. The stand-in doesn't
  decode the request, so the entries name series by their place in it; the checks take them to
  be its first N series. An empty object restores the default. `GET /health` answers `OK`.

Each request line records the path and the query string apart (`path`, `query`), the raw body
(`body_b64`), and whether the write counts as stored:
`apply` when the cue sets it, else whether the answer is `2xx`. A `reset` reads the whole
request, then closes the connection with an RST and no response. The line is printed after the
answer is written, so a request the sink saw answered is in the record unless the process died
between the two. The checks decode the bodies on the host (`soaklib/standin.py`).

SIGTERM closes the data port's listening socket at once, so a connection that arrives after it is
refused, which a sink reads as a connect failure, never as a reset after its request was sent.
It then stops both servers and lets each request in flight finish.
"""

import argparse
import base64
import http.server
import json
import signal
import socket
import struct
import sys
import threading
import time
import urllib.parse

DEFAULT_STATUS = {"remote_write": 200, "datadog": 202, "influxdb": 204}
DEFAULT_PATH = {"remote_write": "/api/v1/write", "datadog": "/api/v2/series",
                "influxdb": "/api/v2/write"}
# Statuses whose answer has no body and no `Content-Length` (RFC 9110, section 8.6).
NO_BODY_STATUSES = (204, 304)
# The cue key only the `datadog` shape takes.
DATADOG_SHAPE = "datadog"
# How long a keep-alive connection may sit idle before its thread lets it go, which bounds a
# SIGTERM's wait for the threads; `--idle-timeout` overrides it.
IDLE_TIMEOUT_S = 5

_print_lock = threading.Lock()


def emit(record):
    with _print_lock:
        sys.stdout.write(json.dumps(record, sort_keys=True) + "\n")
        sys.stdout.flush()


class State:
    def __init__(self, shape, path):
        self.shape = shape
        self.path = path
        self.lock = threading.Lock()
        self.cue = {}
        # The writes a cue with `count` has left to answer; None for a cue without one.
        self.left = None
        self.seq = 0
        self.stopping = False

    def set_cue(self, cue):
        with self.lock:
            self.cue = dict(cue)
            self.left = cue.get("count")

    def take(self, write):
        """The cue in force and the next request number. A `write` to the stand-in's path spends
        one of a counted cue's answers, and the last one clears the cue."""
        with self.lock:
            self.seq += 1
            cue = dict(self.cue)
            if write and self.left is not None:
                self.left -= 1
                if self.left <= 0:
                    self.cue, self.left = {}, None
            return cue, self.seq


def cue_problems(cue, shape="remote_write"):
    if not isinstance(cue, dict):
        return ["a cue is a JSON object"]
    if not cue:
        return []
    problems = []
    unknown = set(cue) - {"step", "status", "body", "answer", "apply", "series_errors", "count"}
    if unknown:
        problems.append(f"unknown keys {sorted(unknown)}")
    if not isinstance(cue.get("step"), str) or not cue.get("step"):
        problems.append("`step` must name the step")
    has_status, has_answer = "status" in cue, "answer" in cue
    if has_status == has_answer:
        problems.append("a cue sets one of `status` and `answer`")
    if has_status and (not isinstance(cue["status"], int) or isinstance(cue["status"], bool)
                       or not 100 <= cue["status"] <= 599):
        problems.append("`status` must be an integer from 100 to 599")
    if has_answer and cue["answer"] != "reset":
        problems.append("`answer` must be \"reset\"")
    if "body" in cue and (not has_status or not isinstance(cue["body"], str)):
        problems.append("`body` is a string, with `status` only")
    if "apply" in cue and not isinstance(cue["apply"], bool):
        problems.append("`apply` must be true or false")
    if "count" in cue and (not isinstance(cue["count"], int) or isinstance(cue["count"], bool)
                           or cue["count"] < 1):
        problems.append("`count` must be a positive integer")
    if "series_errors" in cue:
        count = cue["series_errors"]
        if shape != DATADOG_SHAPE:
            problems.append(f"`series_errors` is for the {DATADOG_SHAPE} shape")
        if not isinstance(count, int) or isinstance(count, bool) or count < 1:
            problems.append("`series_errors` must be a positive integer")
        if "body" in cue:
            problems.append("`series_errors` writes the body, so it takes no `body`")
        status = cue.get("status")
        if not (isinstance(status, int) and not isinstance(status, bool)
                and 200 <= status < 300):
            problems.append("`series_errors` goes with a 2xx `status`")
    return problems


def _varint(n):
    out = bytearray()
    while True:
        out.append((n & 0x7F) | (0x80 if n > 0x7F else 0))
        n >>= 7
        if not n:
            return bytes(out)


def series_errors_body(count):
    """A series `2xx` body naming `count` dropped series: field 1, `repeated string`, one entry
    each, worded as the intake words a validation drop."""
    out = bytearray()
    for index in range(1, count + 1):
        entry = (f"Payload validation failed: series {index} of this request was dropped by "
                 "the soak stand-in (series_errors)").encode()
        out += _varint(1 << 3 | 2) + _varint(len(entry)) + entry
    return bytes(out)


def data_handler(state, idle_timeout=IDLE_TIMEOUT_S):
    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        timeout = idle_timeout

        def log_message(self, *args):
            pass

        def do_POST(self):
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length) if length else b""
            split = urllib.parse.urlsplit(self.path)
            cue, seq = state.take(split.path == state.path)
            record = {
                "event": "request", "seq": seq, "t": time.time(), "path": split.path,
                "query": split.query,
                "content_type": self.headers.get("Content-Type"),
                "content_encoding": self.headers.get("Content-Encoding"),
                "bytes": len(body), "cue": cue.get("step"),
                "body_b64": base64.b64encode(body).decode(),
            }
            if split.path != state.path:
                record.update(answer="status", status=404, stored=False)
                self._answer(404, f"no write path {split.path}; this stand-in takes {state.path}")
            elif cue.get("answer") == "reset":
                record.update(answer="reset", status=None, stored=bool(cue.get("apply", False)))
                self._reset()
            else:
                status = cue.get("status", DEFAULT_STATUS[state.shape])
                stored = cue.get("apply", 200 <= status < 300)
                record.update(answer="status", status=status, stored=bool(stored))
                if "series_errors" in cue:
                    record["series_errors"] = cue["series_errors"]
                    self._answer(status, series_errors_body(cue["series_errors"]),
                                 "application/x-protobuf")
                else:
                    self._answer(status, cue.get("body", "").encode())
            if state.stopping:
                self.close_connection = True
            emit(record)

        def _answer(self, status, payload, content_type="text/plain; charset=utf-8"):
            if isinstance(payload, str):
                payload = payload.encode()
            self.send_response(status)
            if status in NO_BODY_STATUSES or status < 200:
                self.end_headers()
                self.wfile.flush()
                return
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            self.wfile.flush()

        def _reset(self):
            # SO_LINGER with a zero timeout makes close() send an RST. It closes here, because
            # socketserver's own close calls shutdown(SHUT_WR) first, which sends a FIN the
            # client would read as an empty response. `rfile` holds a reference to the socket,
            # so it closes first, or close() leaves the descriptor open.
            self.connection.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                                       struct.pack("ii", 1, 0))
            self.close_connection = True
            self.rfile.close()
            self.connection.close()

    return Handler


def control_handler(state):
    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def _reply(self, status, obj):
            payload = (json.dumps(obj) + "\n").encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self):
            if self.path == "/health":
                # Plain `OK`, as VictoriaMetrics answers, so the driver's one health probe
                # reads both.
                payload = b"OK"
                self.send_response(200)
                self.send_header("Content-Type", "text/plain")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
            elif self.path == "/cue":
                with state.lock:
                    self._reply(200, state.cue)
            else:
                self._reply(404, {"error": f"no {self.path}"})

        def do_POST(self):
            if self.path != "/cue":
                self._reply(404, {"error": f"no {self.path}"})
                return
            length = int(self.headers.get("Content-Length") or 0)
            try:
                cue = json.loads(self.rfile.read(length) or b"{}")
            except ValueError as err:
                self._reply(400, {"error": f"not JSON: {err}"})
                return
            problems = cue_problems(cue, state.shape)
            if problems:
                self._reply(400, {"error": "; ".join(problems)})
                return
            state.set_cue(cue)
            emit({"event": "cue", "t": time.time(), "cue": cue})
            self._reply(200, cue)

    return Handler


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--shape", choices=sorted(DEFAULT_STATUS), default="remote_write")
    parser.add_argument("--path", help="the write path; the shape's own by default")
    parser.add_argument("--data-port", type=int, default=8080)
    parser.add_argument("--control-port", type=int, default=8081)
    # A client that writes on a pooled connection as this side's idle close reaches it sees a
    # reset, an ambiguous answer no cue gave. Over a delayed link that race window is the
    # delay, so a scenario that delays the SUT's egress sets this above the client's own pool
    # idle timeout (90 s for `reqwest`).
    parser.add_argument("--idle-timeout", type=float, default=IDLE_TIMEOUT_S,
                        help="seconds a keep-alive connection may sit idle")
    args = parser.parse_args(argv)
    state = State(args.shape, args.path or DEFAULT_PATH[args.shape])
    data = http.server.ThreadingHTTPServer(("0.0.0.0", args.data_port),
                                           data_handler(state, args.idle_timeout))
    control = http.server.ThreadingHTTPServer(("0.0.0.0", args.control_port),
                                              control_handler(state))
    # Joined on close, so a SIGTERM waits for each request in flight.
    data.daemon_threads = False
    data.block_on_close = True

    def stop(signum, frame):
        state.stopping = True
        # Before anything waits: until `serve_forever` returns, the kernel would go on
        # completing handshakes into the backlog, and each would be reset when the server
        # closes. close() alone isn't enough, because `serve_forever`'s poll holds the socket
        # open until it wakes; shutdown() stops the listener now. `server_close` closes it
        # again, which is harmless.
        try:
            data.socket.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        data.socket.close()
        threading.Thread(target=lambda: (data.shutdown(), control.shutdown())).start()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    threads = [threading.Thread(target=server.serve_forever) for server in (data, control)]
    for thread in threads:
        thread.start()
    emit({"event": "ready", "t": time.time(), "shape": args.shape, "path": state.path})
    for thread in threads:
        thread.join()
    data.server_close()
    control.server_close()
    emit({"event": "stopped", "t": time.time()})
    return 0


if __name__ == "__main__":
    sys.exit(main())
