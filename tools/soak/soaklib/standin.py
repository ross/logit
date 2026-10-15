"""Reads a stand-in destination's record (`tools/soak/standin/standin.py`) and turns it into the
ledger's V.

The record is the stand-in's stdout: one JSON object per line, `event` `request` for each write
it answered (with the raw body, base64), `cue` for each cue it took, and `ready`/`stopped`
around a life. Decoding happens here, on the host, so `check` re-scores a run from the record
alone.

A `remote_write` body is a Snappy **block** (never framed) holding a `prometheus.WriteRequest`
(1.0): `timeseries` is field 1; a `TimeSeries` has `labels` (1) and `samples` (2); a `Label` has
`name` (1) and `value` (2); a `Sample` has `value` (1, a double) and `timestamp` (2, an int64 of
milliseconds). Every other field, `metadata` included, is skipped by wire type.

V is what the stand-in stored: every sample of every request it marked `stored`, in record
order, with a later write of one `(label set, timestamp)` replacing an earlier one, as a
remote-write receiver overwrites. The result has `/api/v1/export`'s shape, so `vm.totals_by_life`
reads it as it reads VictoriaMetrics.
"""

import base64
import json
import math
import struct

REMOTE_WRITE_PATH = "/api/v1/write"


class DecodeError(Exception):
    pass


def _varint(data, pos):
    result = 0
    shift = 0
    while True:
        if pos >= len(data):
            raise DecodeError("truncated varint")
        byte = data[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return result, pos
        shift += 7
        if shift > 63:
            raise DecodeError("varint over 64 bits")


def snappy_decompress(data):
    """The bytes a Snappy block holds: a varint of the uncompressed length, then literal and
    copy elements. Raises `DecodeError` for a malformed block or a length mismatch."""
    length, pos = _varint(data, 0)
    out = bytearray()
    while pos < len(data):
        tag = data[pos]
        pos += 1
        kind = tag & 0x03
        if kind == 0:
            size = tag >> 2
            if size >= 60:
                extra = size - 59
                if pos + extra > len(data):
                    raise DecodeError("truncated literal length")
                size = int.from_bytes(data[pos:pos + extra], "little")
                pos += extra
            size += 1
            if pos + size > len(data):
                raise DecodeError("truncated literal")
            out += data[pos:pos + size]
            pos += size
            continue
        if kind == 1:
            if pos >= len(data):
                raise DecodeError("truncated copy")
            size = ((tag >> 2) & 0x07) + 4
            offset = ((tag >> 5) << 8) | data[pos]
            pos += 1
        else:
            width = 2 if kind == 2 else 4
            if pos + width > len(data):
                raise DecodeError("truncated copy")
            size = (tag >> 2) + 1
            offset = int.from_bytes(data[pos:pos + width], "little")
            pos += width
        if offset == 0 or offset > len(out):
            raise DecodeError(f"copy offset {offset} outside {len(out)} decoded byte(s)")
        # A copy may overlap its own output, so it runs byte by byte.
        start = len(out) - offset
        for index in range(size):
            out.append(out[start + index])
    if len(out) != length:
        raise DecodeError(f"block says {length} byte(s), decoded {len(out)}")
    return bytes(out)


def snappy_compress_literal(data):
    """A valid Snappy block of `data` as literals only, for fixtures: no copy elements."""
    out = bytearray()
    n = len(data)
    while True:
        out.append((n & 0x7F) | (0x80 if n > 0x7F else 0))
        n >>= 7
        if not n:
            break
    pos = 0
    while pos < len(data):
        chunk = data[pos:pos + 65536]
        size = len(chunk) - 1
        if size < 60:
            out.append(size << 2)
        elif size < 256:
            out += bytes([60 << 2, size])
        else:
            out += bytes([61 << 2]) + size.to_bytes(2, "little")
        out += chunk
        pos += len(chunk)
    return bytes(out)


def _fields(data):
    """`(field number, wire type, value)` for each field of a protobuf message: an int for a
    varint, bytes for a length-delimited field, raw bytes for a fixed 32 or 64."""
    pos = 0
    while pos < len(data):
        key, pos = _varint(data, pos)
        number, wire = key >> 3, key & 0x07
        if wire == 0:
            value, pos = _varint(data, pos)
        elif wire == 1:
            value, pos = data[pos:pos + 8], pos + 8
        elif wire == 2:
            size, pos = _varint(data, pos)
            value, pos = data[pos:pos + size], pos + size
        elif wire == 5:
            value, pos = data[pos:pos + 4], pos + 4
        else:
            raise DecodeError(f"unsupported wire type {wire}")
        if pos > len(data):
            raise DecodeError("truncated field")
        yield number, wire, value


def _int64(value):
    return value - (1 << 64) if value >= 1 << 63 else value


def write_request(data):
    """`[(labels, [(timestamp_ms, value)])]` from an uncompressed `WriteRequest`, `labels` a
    dict with `__name__` among them."""
    series = []
    for number, wire, value in _fields(data):
        if number != 1 or wire != 2:
            continue
        labels = {}
        samples = []
        for inner, inner_wire, item in _fields(value):
            if inner == 1 and inner_wire == 2:
                name = text = ""
                for field, field_wire, part in _fields(item):
                    if field == 1 and field_wire == 2:
                        name = part.decode()
                    elif field == 2 and field_wire == 2:
                        text = part.decode()
                labels[name] = text
            elif inner == 2 and inner_wire == 2:
                sample_value, stamp = 0.0, 0
                for field, field_wire, part in _fields(item):
                    if field == 1 and field_wire == 1:
                        sample_value = struct.unpack("<d", part)[0]
                    elif field == 2 and field_wire == 0:
                        stamp = _int64(part)
                samples.append((stamp, sample_value))
        series.append((labels, samples))
    return series


def encode_write_request(series):
    """The inverse of `write_request`, for fixtures: `[(labels, [(timestamp_ms, value)])]` as
    an uncompressed `WriteRequest`."""
    def varint(n):
        out = bytearray()
        n &= (1 << 64) - 1
        while True:
            out.append((n & 0x7F) | (0x80 if n > 0x7F else 0))
            n >>= 7
            if not n:
                return bytes(out)

    def field(number, payload):
        return varint(number << 3 | 2) + varint(len(payload)) + payload

    out = bytearray()
    for labels, samples in series:
        one = bytearray()
        for name, text in sorted(labels.items()):
            one += field(1, field(1, name.encode()) + field(2, text.encode()))
        for stamp, value in samples:
            one += field(2, varint(1 << 3 | 1) + struct.pack("<d", value)
                         + varint(2 << 3 | 0) + varint(stamp))
        out += field(1, bytes(one))
    return bytes(out)


def read_record(text):
    """`(requests, cues, unparsed)`: the record's request lines and cue lines in order, and the
    count of lines that aren't JSON objects."""
    requests, cues, unparsed = [], [], 0
    for line in (text or "").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
        except ValueError:
            unparsed += 1
            continue
        if not isinstance(entry, dict):
            unparsed += 1
        elif entry.get("event") == "request":
            requests.append(entry)
        elif entry.get("event") == "cue":
            cues.append(entry)
    return requests, cues, unparsed


def decode_body(request):
    """The series of one request line's body, raising `DecodeError` for a body that isn't a
    Snappy-compressed `WriteRequest`."""
    try:
        raw = base64.b64decode(request.get("body_b64") or "", validate=True)
    except ValueError as err:
        raise DecodeError(f"body_b64: {err}") from None
    if request.get("content_encoding") != "snappy":
        raise DecodeError(f"Content-Encoding {request.get('content_encoding')!r}, not snappy")
    return write_request(snappy_decompress(raw))


def series_key(labels):
    return tuple(sorted(labels.items()))


def stored_series(requests):
    """`(series, rewrites, conflicts, undecodable)` from the record's requests in order.

    `series` is export-shaped (`{"metric", "values", "timestamps"}`, timestamps in ms) over the
    stored requests, one sample per `(label set, timestamp)`, the last write's. `rewrites` counts
    the samples a later stored write sent again with the same value, `conflicts` lists
    `(series, timestamp, first, last)` for one sent again with a different value, and
    `undecodable` lists `(seq, reason)` for each stored body that didn't decode."""
    by_series = {}
    rewrites = 0
    conflicts = []
    undecodable = []
    for request in requests:
        if not request.get("stored"):
            continue
        try:
            decoded = decode_body(request)
        except DecodeError as err:
            undecodable.append((request.get("seq"), str(err)))
            continue
        for labels, samples in decoded:
            key = series_key(labels)
            points = by_series.setdefault(key, {})
            for stamp, value in samples:
                if stamp in points:
                    first = points[stamp]
                    same = first == value or (math.isnan(first) and math.isnan(value))
                    if same:
                        rewrites += 1
                    else:
                        conflicts.append((key, stamp, first, value))
                points[stamp] = value
    series = []
    for key, points in sorted(by_series.items()):
        stamps = sorted(points)
        series.append({"metric": dict(key), "timestamps": stamps,
                       "values": [points[stamp] for stamp in stamps]})
    return series, rewrites, conflicts, undecodable


def acknowledged_unstored(requests, series):
    """`(series key, timestamp)` for each sample of a request the stand-in answered `2xx` that
    `series` (from `stored_series`) doesn't hold: a write the sink saw accepted that the
    destination never kept. A body that doesn't decode is left to the caller."""
    stored = {(series_key(one["metric"]), stamp) for one in series
              for stamp in one["timestamps"]}
    missing = []
    for request in requests:
        if status_class(request) != "2xx":
            continue
        try:
            decoded = decode_body(request)
        except DecodeError:
            continue
        for labels, samples in decoded:
            key = series_key(labels)
            missing += [(key, stamp) for stamp, _ in samples if (key, stamp) not in stored]
    return missing


def status_class(request):
    """The request's answer in `logit.output.requests`' vocabulary: `2xx` to `5xx` by status,
    `network_error` for a reset."""
    if request.get("answer") == "reset":
        return "network_error"
    status = request.get("status")
    if isinstance(status, int) and 100 <= status <= 599:
        return f"{status // 100}xx"
    return "other"
