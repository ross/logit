"""Reads a stand-in destination's record (`tools/soak/standin/standin.py`) and turns it into the
ledger's V.

The record is the stand-in's stdout: one JSON object per line, `event` `request` for each write
it answered (with the raw body, base64), `cue` for each cue it took, and `ready`/`stopped`
around a life. Decoding happens here, on the host, so `check` re-scores a run from the record
alone.

Two shapes, each the stand-in's `--shape` and a scenario's `[target] shape`:

- `remote_write`: a body is a Snappy **block** (never framed) holding a `prometheus.WriteRequest`
  (1.0): `timeseries` is field 1; a `TimeSeries` has `labels` (1) and `samples` (2); a `Label`
  has `name` (1) and `value` (2); a `Sample` has `value` (1, a double) and `timestamp` (2, an
  int64 of milliseconds). V is every sample of every request the stand-in marked `stored`, in
  record order, with a later write of one `(label set, timestamp)` replacing an earlier one, as a
  remote-write receiver overwrites (`stored_series`).
- `datadog`: a body is the series route's `MetricPayload`
  (`crates/logit-proto/proto/datadog/agent-payload/proto/metrics/agent_payload.proto`), under
  `Content-Encoding` `gzip`, `deflate` (zlib), or none: `series` is field 1; a `MetricSeries` has
  `resources` (1), `metric` (2), `tags` (3, repeated string), `points` (4), `type` (5), and
  `interval` (8); a `Resource` has `type` (1) and `name` (2); a `MetricPoint` has `value` (1, a
  double) and `timestamp` (2, an int64 of seconds). Datadog documents no deduplication of a
  `count` point, so a resend is stored again and adds: V is the sum of every stored point, each
  copy of one `(series, timestamp, value)` past the first counted as a duplicate rather than
  summed, as `topic.py` does for a Kafka record, so a duplicate can't cancel a loss of the same
  size (`stored_counts`). A `2xx` with `series_errors` N stored every series of its request but
  the first N.

Every other field, `metadata` included, is skipped by wire type. V has `/api/v1/export`'s shape,
timestamps in milliseconds, so `ledger.windows` reads it as it reads VictoriaMetrics.
"""

import base64
import gzip
import json
import math
import struct
import zlib
from dataclasses import dataclass, field

REMOTE_WRITE, DATADOG = "remote_write", "datadog"
SHAPES = (REMOTE_WRITE, DATADOG)
REMOTE_WRITE_PATH = "/api/v1/write"
DATADOG_SERIES_PATH = "/api/v2/series"
# Each shape's write path and the status it answers with no cue; standin/standin.py's
# DEFAULT_PATH and DEFAULT_STATUS.
PATHS = {REMOTE_WRITE: REMOTE_WRITE_PATH, DATADOG: DATADOG_SERIES_PATH}
DEFAULT_STATUS = {REMOTE_WRITE: 200, DATADOG: 202}
# `MetricPayload.MetricType`'s names.
DATADOG_TYPES = {0: "unspecified", 1: "count", 2: "rate", 3: "gauge"}


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


def metric_payload(data):
    """`[{metric, tags, resources, points, type, interval}]` from an uncompressed
    `MetricPayload`, one per `MetricSeries` in body order: `tags` a list of strings,
    `resources` a list of `(type, name)`, `points` a list of `(timestamp_s, value)`, `type` a
    `DATADOG_TYPES` name."""
    series = []
    for number, wire, value in _fields(data):
        if number != 1 or wire != 2:
            continue
        one = {"metric": "", "tags": [], "resources": [], "points": [], "type": "unspecified",
               "interval": 0}
        for inner, inner_wire, item in _fields(value):
            if inner == 1 and inner_wire == 2:
                kind = name = ""
                for part_number, part_wire, part in _fields(item):
                    if part_number == 1 and part_wire == 2:
                        kind = part.decode()
                    elif part_number == 2 and part_wire == 2:
                        name = part.decode()
                one["resources"].append((kind, name))
            elif inner == 2 and inner_wire == 2:
                one["metric"] = item.decode()
            elif inner == 3 and inner_wire == 2:
                one["tags"].append(item.decode())
            elif inner == 4 and inner_wire == 2:
                point_value, stamp = 0.0, 0
                for part_number, part_wire, part in _fields(item):
                    if part_number == 1 and part_wire == 1:
                        point_value = struct.unpack("<d", part)[0]
                    elif part_number == 2 and part_wire == 0:
                        stamp = _int64(part)
                one["points"].append((stamp, point_value))
            elif inner == 5 and inner_wire == 0:
                one["type"] = DATADOG_TYPES.get(item, str(item))
            elif inner == 8 and inner_wire == 0:
                one["interval"] = _int64(item)
        series.append(one)
    return series


def encode_metric_payload(series):
    """The inverse of `metric_payload`, for fixtures, with no `type` or `interval` written when
    it is the default."""
    def varint(n):
        out = bytearray()
        n &= (1 << 64) - 1
        while True:
            out.append((n & 0x7F) | (0x80 if n > 0x7F else 0))
            n >>= 7
            if not n:
                return bytes(out)

    def field_bytes(number, payload):
        return varint(number << 3 | 2) + varint(len(payload)) + payload

    codes = {name: code for code, name in DATADOG_TYPES.items()}
    out = bytearray()
    for one in series:
        body = bytearray()
        for kind, name in one.get("resources", []):
            body += field_bytes(1, field_bytes(1, kind.encode()) + field_bytes(2, name.encode()))
        body += field_bytes(2, one["metric"].encode())
        for tag in one.get("tags", []):
            body += field_bytes(3, tag.encode())
        for stamp, value in one.get("points", []):
            body += field_bytes(4, varint(1 << 3 | 1) + struct.pack("<d", value)
                                + varint(2 << 3 | 0) + varint(stamp))
        code = codes.get(one.get("type", "unspecified"), 0)
        if code:
            body += varint(5 << 3 | 0) + varint(code)
        if one.get("interval"):
            body += varint(8 << 3 | 0) + varint(one["interval"])
        out += field_bytes(1, bytes(body))
    return bytes(out)


def datadog_labels(one):
    """A `metric_payload` series as an export's `metric` labels: its name, type, tags, and
    resources, which together name the series a point belongs to."""
    return {"__name__": one["metric"], "type": one["type"],
            "tags": ",".join(sorted(one["tags"])),
            "resources": ",".join(f"{kind}:{name}" for kind, name in sorted(one["resources"]))}


def decompress(raw, encoding):
    """A `datadog_out` body as sent under `Content-Encoding` `encoding`: gzip, deflate (zlib, as
    Datadog means it), or none."""
    try:
        if encoding == "gzip":
            return gzip.decompress(raw)
        if encoding == "deflate":
            return zlib.decompress(raw)
    except (OSError, EOFError, zlib.error) as err:
        raise DecodeError(f"{encoding}: {err}") from None
    if encoding in (None, "", "identity"):
        return raw
    raise DecodeError(f"Content-Encoding {encoding!r}, not gzip, deflate, or none")


def _raw_body(request):
    try:
        return base64.b64decode(request.get("body_b64") or "", validate=True)
    except ValueError as err:
        raise DecodeError(f"body_b64: {err}") from None


def decode_body(request, shape=REMOTE_WRITE):
    """The series of one request line's body as `[(labels, [(timestamp_ms, value)])]`, raising
    `DecodeError` for a body that isn't the shape's: a Snappy-compressed `WriteRequest` for
    `remote_write`, a `MetricPayload` under gzip, deflate, or no compression for `datadog`."""
    raw = _raw_body(request)
    if shape == DATADOG:
        decoded = metric_payload(decompress(raw, request.get("content_encoding")))
        return [(datadog_labels(one), [(stamp * 1000, value) for stamp, value in one["points"]])
                for one in decoded]
    if request.get("content_encoding") != "snappy":
        raise DecodeError(f"Content-Encoding {request.get('content_encoding')!r}, not snappy")
    return write_request(snappy_decompress(raw))


def named_series(request, decoded):
    """How many series of a request's decoded body a `series_errors` answer named: its first N,
    or all of them when the body holds fewer. 0 for any other answer."""
    count = request.get("series_errors")
    if status_class(request) != "2xx" or not isinstance(count, int) or count < 1:
        return 0
    return min(count, len(decoded))


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


@dataclass
class Counts:
    """What a `datadog` stand-in stored (`stored_counts`): `series` export-shaped, each
    `(series, timestamp)`'s value the sum of its distinct copies; `points` one `(timestamp_s,
    value)` per distinct copy, V's terms; `duplicates` one `(series key, timestamp_ms, value,
    first index, copy index)` per copy of a point already stored, each index the request's place
    in the record, since `seq` starts again at 1 when the stand-in restarts; `conflicts` one `(series key,
    timestamp_ms, first value, later value)` per point stored again with another value;
    `undecodable` one `(seq, reason)` per stored body that doesn't decode; and `named` one
    `(seq, count, t)` per `series_errors` answer: the series it named and when it was answered,
    epoch seconds."""

    series: list = field(default_factory=list)
    points: list = field(default_factory=list)
    duplicates: list = field(default_factory=list)
    conflicts: list = field(default_factory=list)
    undecodable: list = field(default_factory=list)
    named: list = field(default_factory=list)


def stored_counts(requests):
    """A `datadog` stand-in's record as `Counts`, over the stored requests in record order. A
    `series_errors` answer's first N series aren't stored."""
    counts = Counts()
    by_series = {}
    first = {}
    for index, request in enumerate(requests):
        if not request.get("stored"):
            continue
        try:
            decoded = decode_body(request, DATADOG)
        except DecodeError as err:
            counts.undecodable.append((request.get("seq"), str(err)))
            continue
        named = named_series(request, decoded)
        if named:
            counts.named.append((request.get("seq"), named, request.get("t")))
        for labels, samples in decoded[named:]:
            key = series_key(labels)
            points = by_series.setdefault(key, {})
            for stamp, value in samples:
                seen = first.setdefault((key, stamp), [])
                same = next((kept_at for kept, kept_at in seen if kept == value
                             or (math.isnan(kept) and math.isnan(value))), None)
                if same is not None:
                    counts.duplicates.append((key, stamp, value, same, index))
                    continue
                if seen:
                    counts.conflicts.append((key, stamp, seen[0][0], value))
                seen.append((value, index))
                points[stamp] = points.get(stamp, 0.0) + value
                counts.points.append((stamp / 1000, value))
    for key, points in sorted(by_series.items()):
        stamps = sorted(points)
        counts.series.append({"metric": dict(key), "timestamps": stamps,
                              "values": [points[stamp] for stamp in stamps]})
    return counts


def acknowledged_unstored(requests, series, shape=REMOTE_WRITE):
    """`(series key, timestamp)` for each sample of a request the stand-in answered `2xx` that
    `series` (from `stored_series` or `stored_counts`) doesn't hold: a write the sink saw
    accepted that the destination never kept. The series a `series_errors` answer named are
    left out, since the sink saw them dropped, as is a body that doesn't decode, which the
    caller judges."""
    stored = {(series_key(one["metric"]), stamp) for one in series
              for stamp in one["timestamps"]}
    missing = []
    for request in requests:
        if status_class(request) != "2xx":
            continue
        try:
            decoded = decode_body(request, shape)
        except DecodeError:
            continue
        for labels, samples in decoded[named_series(request, decoded):]:
            key = series_key(labels)
            missing += [(key, stamp) for stamp, _ in samples if (key, stamp) not in stored]
    return missing


def datadog_fault_class(request):
    """The fault class `datadog_out` reads a request's answer as (crates/logit-outputs/src/
    datadog.rs, "Faults, retries, and duplicate safety"'s table): None for a `2xx`; `ambiguous`
    for `408`, `429`, any `5xx` (`501` included), and a reset; `refused` for `401`, `403`,
    `404`, `405`, and `407`; `rejected` for `400`, `413`, and any other `3xx` or `4xx`. A
    connect failure is `clean` and never reaches the record."""
    if request.get("answer") == "reset":
        return "ambiguous"
    status = request.get("status")
    if not isinstance(status, int) or 200 <= status < 300:
        return None
    if status in (408, 429) or 500 <= status <= 599:
        return "ambiguous"
    if status in (401, 403, 404, 405, 407):
        return "refused"
    return "rejected"


def points_in(request, shape):
    """The points in a request's body, or 0 when it doesn't decode."""
    try:
        return sum(len(samples) for _, samples in decode_body(request, shape))
    except DecodeError:
        return 0


def status_class(request):
    """The request's answer in `logit.output.requests`' vocabulary: `2xx` to `5xx` by status,
    `network_error` for a reset."""
    if request.get("answer") == "reset":
        return "network_error"
    status = request.get("status")
    if isinstance(status, int) and 100 <= status <= 599:
        return f"{status // 100}xx"
    return "other"
