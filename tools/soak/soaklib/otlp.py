"""A stdlib protobuf walker for one OTLP `ExportMetricsServiceRequest`, as `kafka_out` writes
one per record under `format: otlp_proto`.

It reads the protobuf wire format (varint, 64-bit, length-delimited, and 32-bit fields) and
decodes the request only as deep as the ledger needs: each `Sum` and `Gauge` data point's metric
name, attributes, `time_unix_nano`, and value, under its resource. Field numbers are the ones
`opentelemetry/proto/metrics/v1/metrics.proto` gives, which
`crates/logit-proto/src/otlp/generated/metrics.v1.rs` carries.

A decoder reads protobuf as prost does:

- A field it doesn't read is skipped by its wire type, so a newer field, a histogram's packed or
  unpacked bucket counts, and an exemplar cost nothing.
- A scalar field that appears twice keeps its last value. A `oneof` keeps its last member, so a
  `Metric` holding `gauge` and then `sum` is a `Sum`. A singular message field that appears twice
  is merged, read as the concatenation of its occurrences.
- A truncated field, a varint longer than ten bytes, a group wire type, field number 0, or a
  field the walker reads arriving under the wrong wire type raises `ProtoError`.

None of the fields the walker reads is a repeated scalar, so the packed and unpacked forms of a
repeated field only arise in fields it skips.
"""

import struct
from dataclasses import dataclass

VARINT, I64, LEN, SGROUP, EGROUP, I32 = 0, 1, 2, 3, 4, 5

# ExportMetricsServiceRequest / ResourceMetrics / ScopeMetrics.
REQUEST_RESOURCE_METRICS = 1
RESOURCE_METRICS_RESOURCE = 1
RESOURCE_METRICS_SCOPE_METRICS = 2
SCOPE_METRICS_METRICS = 2
# Metric and its `data` oneof.
METRIC_NAME = 1
METRIC_GAUGE = 5
METRIC_SUM = 7
METRIC_DATA = (5, 7, 9, 10, 11)
# Gauge and Sum.
DATA_POINTS = 1
SUM_TEMPORALITY = 2
SUM_MONOTONIC = 3
# NumberDataPoint and its `value` oneof.
POINT_TIME = 3
POINT_AS_DOUBLE = 4
POINT_AS_INT = 6
POINT_ATTRIBUTES = 7

GAUGE, SUM = "gauge", "sum"
DELTA, CUMULATIVE = 1, 2


class ProtoError(ValueError):
    """Bytes that aren't a well-formed message of the shape the walker reads."""


@dataclass(frozen=True)
class Point:
    """One `NumberDataPoint` of a `Sum` or `Gauge`. `series` identifies its time series: the
    resource's encoded bytes, the metric name, and its attributes' encoded `KeyValue`s, sorted.
    `value` is an int for `as_int`, a float for `as_double`, and 0 for a point with neither, as
    `otlp_in` decodes one."""
    kind: str
    name: str
    series: tuple
    time_unix_nano: int
    value: object
    temporality: int = 0
    monotonic: bool = False


def varint(buf, pos):
    """`(value, next position)` for the varint at `pos`."""
    value = shift = 0
    for i in range(10):
        if pos + i >= len(buf):
            raise ProtoError(f"varint truncated at byte {pos}")
        byte = buf[pos + i]
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value & 0xFFFFFFFFFFFFFFFF, pos + i + 1
        shift += 7
    raise ProtoError(f"varint at byte {pos} is longer than ten bytes")


def fields(buf):
    """Yields `(number, wire type, value)` for each field of the message `buf`: an int for a
    varint, the raw bytes for a 64-bit, 32-bit, or length-delimited field."""
    buf = bytes(buf)
    pos = 0
    while pos < len(buf):
        key, pos = varint(buf, pos)
        number, wire = key >> 3, key & 7
        if number == 0:
            raise ProtoError(f"field number 0 at byte {pos}")
        if wire == VARINT:
            value, pos = varint(buf, pos)
        elif wire in (I64, I32):
            size = 8 if wire == I64 else 4
            if pos + size > len(buf):
                raise ProtoError(f"field {number} truncated")
            value, pos = buf[pos:pos + size], pos + size
        elif wire == LEN:
            size, pos = varint(buf, pos)
            if pos + size > len(buf):
                raise ProtoError(f"field {number}'s {size} bytes are truncated")
            value, pos = buf[pos:pos + size], pos + size
        else:
            raise ProtoError(f"field {number} has wire type {wire}, which no OTLP message uses")
        yield number, wire, value


def _want(number, wire, expected):
    if wire != expected:
        raise ProtoError(f"field {number} has wire type {wire}, expected {expected}")


def _messages(buf, wanted):
    """`{number: [bytes, ...]}` for each length-delimited field in `wanted`."""
    out = {number: [] for number in wanted}
    for number, wire, value in fields(buf):
        if number in out:
            _want(number, wire, LEN)
            out[number].append(value)
    return out


def _point(buf, kind, name, resource, temporality, monotonic):
    time = 0
    value = 0
    attributes = []
    for number, wire, raw in fields(buf):
        if number == POINT_TIME:
            _want(number, wire, I64)
            time = struct.unpack("<Q", raw)[0]
        elif number == POINT_AS_DOUBLE:
            _want(number, wire, I64)
            value = struct.unpack("<d", raw)[0]
        elif number == POINT_AS_INT:
            _want(number, wire, I64)
            value = struct.unpack("<q", raw)[0]
        elif number == POINT_ATTRIBUTES:
            _want(number, wire, LEN)
            list(fields(raw))  # a KeyValue must itself be well formed
            attributes.append(raw)
    series = (resource, name, tuple(sorted(attributes)))
    return Point(kind, name, series, time, value, temporality, monotonic)


def _metric_points(buf, resource):
    name = ""
    data = None  # (number, [occurrences]): the oneof's last member, merged
    for number, wire, value in fields(buf):
        if number == METRIC_NAME:
            _want(number, wire, LEN)
            try:
                name = value.decode("utf-8")
            except UnicodeDecodeError as err:
                raise ProtoError(f"metric name isn't UTF-8: {err}") from None
        elif number in METRIC_DATA:
            _want(number, wire, LEN)
            if data is None or data[0] != number:
                data = (number, [])
            data[1].append(value)
    if data is None or data[0] not in (METRIC_GAUGE, METRIC_SUM):
        return []
    kind = SUM if data[0] == METRIC_SUM else GAUGE
    body = b"".join(data[1])
    temporality, monotonic, points = 0, False, []
    for number, wire, value in fields(body):
        if number == DATA_POINTS:
            _want(number, wire, LEN)
            points.append(value)
        elif kind == SUM and number == SUM_TEMPORALITY:
            _want(number, wire, VARINT)
            temporality = value
        elif kind == SUM and number == SUM_MONOTONIC:
            _want(number, wire, VARINT)
            monotonic = bool(value)
    return [_point(p, kind, name, resource, temporality, monotonic) for p in points]


def metric_points(buf):
    """Every `Sum` and `Gauge` data point in the `ExportMetricsServiceRequest` `buf`, in wire
    order. Raises `ProtoError` for bytes that don't decode."""
    points = []
    for resource_metrics in _messages(buf, [REQUEST_RESOURCE_METRICS])[REQUEST_RESOURCE_METRICS]:
        parts = _messages(resource_metrics,
                          [RESOURCE_METRICS_RESOURCE, RESOURCE_METRICS_SCOPE_METRICS])
        resource = b"".join(parts[RESOURCE_METRICS_RESOURCE])
        list(fields(resource))
        for scope_metrics in parts[RESOURCE_METRICS_SCOPE_METRICS]:
            for metric in _messages(scope_metrics, [SCOPE_METRICS_METRICS])[SCOPE_METRICS_METRICS]:
                points += _metric_points(metric, resource)
    return points


def unescape(line):
    """The bytes `kafka-console-consumer.sh` printed as `line` under
    `--value-deserializer org.apache.kafka.common.serialization.BytesDeserializer`: Kafka's
    `Bytes.toString` writes each byte from space to `~` but the backslash as itself, and every
    other byte as `\\xHH`, uppercase. Raises `ProtoError` for a backslash not followed by that
    form, or a character outside it."""
    out = bytearray()
    i = 0
    while i < len(line):
        ch = line[i]
        if ch == "\\":
            digits = line[i + 2:i + 4]
            if line[i + 1:i + 2] != "x" or len(digits) != 2 or not all(
                    c in "0123456789ABCDEF" for c in digits):
                raise ProtoError(f"bad escape at character {i}")
            out.append(int(digits, 16))
            i += 4
            continue
        if not " " <= ch <= "~":
            raise ProtoError(f"character {ch!r} at {i} isn't in the escaped form")
        out.append(ord(ch))
        i += 1
    return bytes(out)


def escape(data):
    """`unescape`'s inverse, Kafka's `Bytes.toString`."""
    return "".join(chr(b) if 0x20 <= b <= 0x7E and b != 0x5C else f"\\x{b:02X}" for b in data)
