"""A scripted HTTP destination for a soak's SUT sink: it answers each write as the driver last
told it to, and prints one JSON line per request to stdout, the record the checks read.

Stdlib only; compose runs it in a stock Python image. It serves two ports:

- The data port (`--data-port`) takes the sink's writes on `--path`. With no cue it answers the
  shape's default with an empty body: `200` on `/api/v1/write` for `remote_write`, `202` on
  `/api/v2/series` for `datadog`, `204` on `/api/v2/write` for `influxdb`, with no
  `Content-Length`, which a `204` mustn't carry, and `200` on `/v1/metrics` for `otlp`, an empty
  `ExportMetricsServiceResponse`. Under `splunk` it answers `200` on `/services/collector/event`
  with HEC's success body and an `ackId`, minted per `X-Splunk-Request-Channel` from 0, and takes
  `/services/collector/ack` polls too: each id is answered `true` once minted on the poll's
  channel, `false` otherwise. The path is compared without its query string, which
  `influxdb_out` always sends (`org`, `orgID`, `bucket`, `precision`). A request to any other
  path answers `404`, so a misconfigured endpoint can't pass for a stored write.
- The control port (`--control-port`) takes `POST /cue` from the driver: a JSON object with
  `step`, and either `status` (with an optional `body`) or `answer: "reset"`, and an optional
  `apply` and `count`. With `count`, N, the cue answers the next N writes on `--path` and is then
  spent: the stand-in answers the default again, recording no cue, until the next `POST /cue`.
  With several requests in flight, that lands a cue on one of them. Under the `datadog` shape a `2xx` status can carry `series_errors`, N: the answer's
  body is then the series route's `errors` list, protobuf field 1 (`repeated string`), with N
  entries, which `datadog_out` counts as N series the intake dropped. The stand-in doesn't
  decode the request, so the entries name series by their place in it; the checks take them to
  be its first N series. Under the `otlp` shape a `2xx` can carry `partial_success`, N: the body
  is an `ExportMetricsServiceResponse` whose `partial_success` rejects N data points, which the
  checks take to be the request's first N. Under the `splunk` shape a `400` can carry
  `invalid_event`, N: the body is HEC's code 6 naming object N, and the record counts the
  objects before it stored, as Splunk indexes them; and a `2xx` can carry `withhold_acks`, which
  answers `false` to every poll while the cue is in force. With `after`, K, the next K writes
  get the default answer, recorded with no cue, before the cue answers any, so a cue can land on
  the second body of a batch. A cue's `headers`, an object of header names to string values, replaces
  the response headers the stand-in would add itself (below), so `{}` sends none. An empty cue
  restores the default. `GET /health` answers `OK`.

Under the `remote_write` shape, a `2xx` other than `202` to a remote-write 2.0 request (its
`Content-Type` names `io.prometheus.write.v2.Request`) carries
`X-Prometheus-Remote-Write-{Samples,Histograms,Exemplars}-Written` with the body's own counts,
as a 2.0 receiver must: the samples and exemplars in the Snappy-compressed `Request`, and `0`
histograms. A body that doesn't decode gets no headers.

Each request line records the path and the query string apart (`path`, `query`), the raw body
(`body_b64`), the response headers it added (`headers`), and whether the write counts as stored:
`apply` when the cue sets it, else whether the answer is `2xx`. A cued `series_errors`,
`partial_success`, or `invalid_event` is recorded under its own name, a minted `ackId` as
`ack_id`, and a poll's answer as `acks`, each id's string to its boolean. A `reset` reads the whole
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

DEFAULT_STATUS = {"remote_write": 200, "datadog": 202, "influxdb": 204, "otlp": 200,
                  "splunk": 200}
DEFAULT_PATH = {"remote_write": "/api/v1/write", "datadog": "/api/v2/series",
                "influxdb": "/api/v2/write", "otlp": "/v1/metrics",
                "splunk": "/services/collector/event"}
# Statuses whose answer has no body and no `Content-Length` (RFC 9110, section 8.6).
NO_BODY_STATUSES = (204, 304)
# The cue key only the `datadog` shape takes.
DATADOG_SHAPE = "datadog"
REMOTE_WRITE_SHAPE = "remote_write"
OTLP_SHAPE = "otlp"
SPLUNK_SHAPE = "splunk"
SPLUNK_ACK_PATH = "/services/collector/ack"
SPLUNK_CHANNEL_HEADER = "X-Splunk-Request-Channel"
# A remote-write 2.0 request's `Content-Type` names this message.
REMOTE_WRITE_V2_PROTO = "io.prometheus.write.v2.Request"
WRITTEN_HEADERS = ("X-Prometheus-Remote-Write-Samples-Written",
                   "X-Prometheus-Remote-Write-Histograms-Written",
                   "X-Prometheus-Remote-Write-Exemplars-Written")
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
        # The writes a cue with `after` lets through with the default answer first.
        self.skip = 0
        self.seq = 0
        self.stopping = False
        # Under `splunk`, the next `ackId` per channel, and the ids each channel was given.
        self.next_ack = {}
        self.minted = {}

    def set_cue(self, cue):
        with self.lock:
            self.cue = dict(cue)
            self.left = cue.get("count")
            self.skip = cue.get("after", 0)

    def take(self, write):
        """The cue in force and the next request number. A `write` to the stand-in's path first
        spends a cue's `after`, getting no cue, then one of a counted cue's answers, and the last
        one clears the cue."""
        with self.lock:
            self.seq += 1
            if write and self.skip > 0:
                self.skip -= 1
                return {}, self.seq
            cue = dict(self.cue)
            if write and self.left is not None:
                self.left -= 1
                if self.left <= 0:
                    self.cue, self.left = {}, None
            return cue, self.seq

    def mint_ack(self, channel):
        with self.lock:
            ack_id = self.next_ack.get(channel, 0)
            self.next_ack[channel] = ack_id + 1
            self.minted.setdefault(channel, set()).add(ack_id)
            return ack_id

    def acked(self, channel, ack_id):
        with self.lock:
            return ack_id in self.minted.get(channel, ())


def _positive(value):
    return isinstance(value, int) and not isinstance(value, bool) and value >= 1


def cue_problems(cue, shape="remote_write"):
    if not isinstance(cue, dict):
        return ["a cue is a JSON object"]
    if not cue:
        return []
    problems = []
    unknown = set(cue) - {"step", "status", "body", "answer", "apply", "series_errors", "count",
                          "headers", "after", "partial_success", "invalid_event",
                          "withhold_acks"}
    if unknown:
        problems.append(f"unknown keys {sorted(unknown)}")
    if not isinstance(cue.get("step"), str) or not cue.get("step"):
        problems.append("`step` must name the step")
    has_status, has_answer = "status" in cue, "answer" in cue
    if has_status == has_answer:
        problems.append("a cue sets one of `status` and `answer`")
    status = cue.get("status")
    valid_status = isinstance(status, int) and not isinstance(status, bool)
    success = valid_status and 200 <= status < 300
    if has_status and (not valid_status or not 100 <= status <= 599):
        problems.append("`status` must be an integer from 100 to 599")
    if has_answer and cue["answer"] != "reset":
        problems.append("`answer` must be \"reset\"")
    if "body" in cue and (not has_status or not isinstance(cue["body"], str)):
        problems.append("`body` is a string, with `status` only")
    if "headers" in cue and (not has_status or not isinstance(cue["headers"], dict) or not all(
            isinstance(name, str) and name and isinstance(value, str)
            for name, value in cue["headers"].items())):
        problems.append("`headers` maps header names to strings, with `status` only")
    if "apply" in cue and not isinstance(cue["apply"], bool):
        problems.append("`apply` must be true or false")
    if "count" in cue and not _positive(cue["count"]):
        problems.append("`count` must be a positive integer")
    if "after" in cue and not _positive(cue["after"]):
        problems.append("`after` must be a positive integer")
    # Each body-writing key: its shape, and the status it goes with.
    for key, own_shape, fits, wanted in (
            ("series_errors", DATADOG_SHAPE, success, "a 2xx `status`"),
            ("partial_success", OTLP_SHAPE, success, "a 2xx `status`"),
            ("invalid_event", SPLUNK_SHAPE, status == 400, "`status` 400")):
        if key not in cue:
            continue
        if shape != own_shape:
            problems.append(f"`{key}` is for the {own_shape} shape")
        if key == "invalid_event":
            if not (isinstance(cue[key], int) and not isinstance(cue[key], bool)
                    and cue[key] >= 0):
                problems.append(f"`{key}` must be an integer of 0 or more")
        elif not _positive(cue[key]):
            problems.append(f"`{key}` must be a positive integer")
        if "body" in cue:
            problems.append(f"`{key}` writes the body, so it takes no `body`")
        if not fits:
            problems.append(f"`{key}` goes with {wanted}")
    if "withhold_acks" in cue:
        if shape != SPLUNK_SHAPE:
            problems.append(f"`withhold_acks` is for the {SPLUNK_SHAPE} shape")
        if cue["withhold_acks"] is not True:
            problems.append("`withhold_acks` must be true")
        if not success:
            problems.append("`withhold_acks` goes with a 2xx `status`")
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


def partial_success_body(count):
    """An `ExportMetricsServiceResponse` whose `partial_success` (field 1) rejects `count` data
    points (`rejected_data_points`, field 1) with an `error_message` (field 2)."""
    message = (f"{count} data point(s) rejected by the soak stand-in "
               "(partial_success)").encode()
    inner = (_varint(1 << 3 | 0) + _varint(count)
             + _varint(2 << 3 | 2) + _varint(len(message)) + message)
    return _varint(1 << 3 | 2) + _varint(len(inner)) + inner


def hec_body(**fields):
    """A HEC answer, `{"text","code"}` and then `fields`, in Splunk's key order and with no
    whitespace, as `crates/logit-proto/src/splunk/response.rs` writes one."""
    return json.dumps(fields, separators=(",", ":")).encode()


def ack_reply(state, channel, body, withhold):
    """`(ids, reply)` for a `/services/collector/ack` poll: each id `true` once minted on
    `channel` and not withheld, else `false`; `ids` is None for a body that isn't
    `{"acks":[<id>,...]}`."""
    try:
        ids = json.loads(body or b"null")["acks"]
    except (ValueError, TypeError, KeyError):
        return None, None
    if not isinstance(ids, list) or not all(
            isinstance(one, int) and not isinstance(one, bool) for one in ids):
        return None, None
    acks = {str(one): (not withhold and state.acked(channel, one)) for one in ids}
    return ids, {"acks": acks}


def _read_varint(data, pos):
    result = shift = 0
    while True:
        byte = data[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return result, pos
        shift += 7
        if shift > 63:
            raise ValueError("varint over 64 bits")


def _snappy_decompress(data):
    """The bytes a Snappy block holds; raises `ValueError` or `IndexError` for a bad one."""
    length, pos = _read_varint(data, 0)
    out = bytearray()
    while pos < len(data):
        tag = data[pos]
        pos += 1
        kind = tag & 0x03
        if kind == 0:
            size = tag >> 2
            if size >= 60:
                size = int.from_bytes(data[pos:pos + size - 59], "little")
                pos += (tag >> 2) - 59
            size += 1
            out += data[pos:pos + size]
            pos += size
            continue
        if kind == 1:
            size, offset = ((tag >> 2) & 0x07) + 4, ((tag >> 5) << 8) | data[pos]
            pos += 1
        else:
            width = 2 if kind == 2 else 4
            size, offset = (tag >> 2) + 1, int.from_bytes(data[pos:pos + width], "little")
            pos += width
        if not 0 < offset <= len(out):
            raise ValueError("copy offset outside the decoded bytes")
        start = len(out) - offset
        for index in range(size):
            out.append(out[start + index])
    if len(out) != length:
        raise ValueError("length mismatch")
    return bytes(out)


def _fields(data):
    """`(field number, value)` for each length-delimited field of a protobuf message, skipping
    the rest by wire type."""
    pos = 0
    while pos < len(data):
        key, pos = _read_varint(data, pos)
        wire = key & 0x07
        if wire == 0:
            _, pos = _read_varint(data, pos)
        elif wire == 1:
            pos += 8
        elif wire == 5:
            pos += 4
        elif wire == 2:
            size, pos = _read_varint(data, pos)
            if pos + size > len(data):
                raise ValueError("truncated field")
            yield key >> 3, data[pos:pos + size]
            pos += size
        else:
            raise ValueError(f"unsupported wire type {wire}")


def written_counts(body):
    """`(samples, exemplars)` in a Snappy-compressed remote-write 2.0 `Request`: `timeseries` is
    field 5, and a `TimeSeries` holds `samples` (2) and `exemplars` (4)."""
    samples = exemplars = 0
    for number, series in _fields(_snappy_decompress(body)):
        if number != 5:
            continue
        for inner, _ in _fields(series):
            samples += inner == 2
            exemplars += inner == 4
    return samples, exemplars


def default_headers(shape, content_type, status, body):
    """The response headers the stand-in adds with no cue `headers`: a 2.0 receiver's written
    counts on a `2xx` other than `202` to a 2.0 request, under the `remote_write` shape."""
    if (shape != REMOTE_WRITE_SHAPE or REMOTE_WRITE_V2_PROTO not in (content_type or "")
            or not 200 <= status < 300 or status == 202):
        return {}
    try:
        samples, exemplars = written_counts(body)
    except (ValueError, IndexError):
        return {}
    return dict(zip(WRITTEN_HEADERS, (str(samples), "0", str(exemplars))))


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
            channel = self.headers.get(SPLUNK_CHANNEL_HEADER) or ""
            if state.shape == SPLUNK_SHAPE and split.path == SPLUNK_ACK_PATH:
                self._ack(record, channel, body, cue)
            elif split.path != state.path:
                record.update(answer="status", status=404, stored=False)
                self._answer(404, f"no write path {split.path}; this stand-in takes {state.path}")
            elif cue.get("answer") == "reset":
                record.update(answer="reset", status=None, stored=bool(cue.get("apply", False)))
                self._reset()
            else:
                status = cue.get("status", DEFAULT_STATUS[state.shape])
                success = 200 <= status < 300
                stored = cue.get("apply", success)
                headers = cue.get("headers")
                if headers is None:
                    headers = default_headers(state.shape, record["content_type"], status, body)
                record.update(answer="status", status=status, stored=bool(stored),
                              headers=headers)
                if "series_errors" in cue:
                    record["series_errors"] = cue["series_errors"]
                    self._answer(status, series_errors_body(cue["series_errors"]),
                                 "application/x-protobuf", headers)
                elif "partial_success" in cue:
                    record["partial_success"] = cue["partial_success"]
                    self._answer(status, partial_success_body(cue["partial_success"]),
                                 "application/x-protobuf", headers)
                elif "invalid_event" in cue:
                    record["invalid_event"] = cue["invalid_event"]
                    self._answer(status, hec_body(**{
                        "text": "Invalid data format", "code": 6,
                        "invalid-event-number": cue["invalid_event"]}),
                        "application/json", headers)
                elif "body" in cue:
                    self._answer(status, cue["body"].encode(), headers=headers)
                elif state.shape == SPLUNK_SHAPE and success:
                    record["ack_id"] = state.mint_ack(channel)
                    self._answer(status, hec_body(text="Success", code=0,
                                                  ackId=record["ack_id"]),
                                 "application/json", headers)
                elif state.shape == OTLP_SHAPE and success:
                    self._answer(status, b"", "application/x-protobuf", headers)
                else:
                    self._answer(status, b"", headers=headers)
            if state.stopping:
                self.close_connection = True
            emit(record)

        def _ack(self, record, channel, body, cue):
            # A poll is no write: it spends no cue, and records the cue only when it withholds.
            withhold = bool(cue.get("withhold_acks"))
            record["cue"] = cue.get("step") if withhold else None
            ids, reply = ack_reply(state, channel, body, withhold)
            if reply is None:
                record.update(answer="status", status=400, stored=False)
                self._answer(400, hec_body(text="Invalid data format", code=6),
                             "application/json")
                return
            record.update(answer="status", status=200, stored=False, acks=reply["acks"])
            self._answer(200, json.dumps(reply, separators=(",", ":")).encode(),
                         "application/json")

        def _answer(self, status, payload, content_type="text/plain; charset=utf-8",
                    headers=None):
            if isinstance(payload, str):
                payload = payload.encode()
            self.send_response(status)
            for name, value in (headers or {}).items():
                self.send_header(name, value)
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
    # delay, so a scenario that delays the SUT's egress keeps the sink's `idle_timeout:` below
    # this less the delay.
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
