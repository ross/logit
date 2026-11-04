"""A stdlib MessagePack reader for the tracer API's bodies, as `datadog_trace_out` writes them:
v0.4's array of traces and v0.7's `TracerPayload` on the trace routes, and a
`ClientStatsPayload` on `/v0.6/stats`.

`unpack` reads one whole value into Python objects: `nil` as None, a bool, an int of any
format, a `float 32` or `float 64` as a float, a `str` as text, a `bin` as bytes, an array as a
list, a map as a dict (a repeated key keeps its last value, as the Agent's generated
`UnmarshalMsg` assigns field by field), and an `ext` as `Ext`. It is bounded, since a body comes
from the system under test: nesting past `MAX_DEPTH`, a container whose declared length couldn't
fit in the bytes left (every element takes at least one), a map key that is a container, a `str`
that isn't UTF-8, the reserved byte `0xc1`, a truncated value, and bytes after the value each
raise `MsgpackError`. The format is the MessagePack specification's
(https://github.com/msgpack/msgpack/blob/master/spec.md); `crates/logit-proto/src/msgpack.rs` is
the writer it reads.
"""

import struct
from dataclasses import dataclass

# The deepest nesting a tracer body reaches is a v0.7 span event's array attribute, about ten
# levels: payload, chunks, chunk, spans, span, span_events, event, attributes, value, array.
MAX_DEPTH = 32


class MsgpackError(ValueError):
    """Bytes that aren't one well-formed MessagePack value within the reader's bounds."""


@dataclass(frozen=True)
class Ext:
    """An `ext` value: its type byte, signed, and its data."""
    code: int
    data: bytes


class _Reader:
    def __init__(self, buf):
        self.buf = bytes(buf)
        self.pos = 0

    def take(self, size):
        end = self.pos + size
        if end > len(self.buf):
            raise MsgpackError(f"{size} byte(s) at byte {self.pos} run past the body's "
                               f"{len(self.buf)}")
        out = self.buf[self.pos:end]
        self.pos = end
        return out

    def uint(self, size):
        return int.from_bytes(self.take(size), "big")

    def sint(self, size):
        return int.from_bytes(self.take(size), "big", signed=True)

    def length(self, size, per_item):
        """A container's declared length, refused when its items couldn't fit."""
        at = self.pos
        count = self.uint(size)
        if count * per_item > len(self.buf) - self.pos:
            raise MsgpackError(f"a container at byte {at - 1} declares {count} item(s), more "
                               f"than the {len(self.buf) - self.pos} byte(s) left hold")
        return count

    def text(self, size):
        raw = self.take(size)
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError as err:
            raise MsgpackError(f"a str at byte {self.pos - size} isn't UTF-8: {err}") from None

    def value(self, depth):
        if depth > MAX_DEPTH:
            raise MsgpackError(f"nesting past {MAX_DEPTH} levels at byte {self.pos}")
        tag = self.take(1)[0]
        if tag <= 0x7F:
            return tag
        if tag >= 0xE0:
            return tag - 0x100
        if 0x80 <= tag <= 0x8F:
            return self.map(tag & 0x0F, depth)
        if 0x90 <= tag <= 0x9F:
            return self.array(tag & 0x0F, depth)
        if 0xA0 <= tag <= 0xBF:
            return self.text(tag & 0x1F)
        if tag == 0xC0:
            return None
        if tag in (0xC2, 0xC3):
            return tag == 0xC3
        if tag in (0xC4, 0xC5, 0xC6):
            return self.take(self.uint(1 << (tag - 0xC4)))
        if tag in (0xC7, 0xC8, 0xC9):
            size = self.uint(1 << (tag - 0xC7))
            return Ext(self.sint(1), self.take(size))
        if tag == 0xCA:
            return struct.unpack(">f", self.take(4))[0]
        if tag == 0xCB:
            return struct.unpack(">d", self.take(8))[0]
        if 0xCC <= tag <= 0xCF:
            return self.uint(1 << (tag - 0xCC))
        if 0xD0 <= tag <= 0xD3:
            return self.sint(1 << (tag - 0xD0))
        if 0xD4 <= tag <= 0xD8:
            code = self.sint(1)
            return Ext(code, self.take(1 << (tag - 0xD4)))
        if tag in (0xD9, 0xDA, 0xDB):
            return self.text(self.uint(1 << (tag - 0xD9)))
        if tag in (0xDC, 0xDD):
            return self.array(self.length(2 if tag == 0xDC else 4, 1), depth)
        if tag in (0xDE, 0xDF):
            return self.map(self.length(2 if tag == 0xDE else 4, 2), depth)
        raise MsgpackError(f"byte 0x{tag:02x} at {self.pos - 1} is no MessagePack format")

    def array(self, count, depth):
        return [self.value(depth + 1) for _ in range(count)]

    def map(self, count, depth):
        out = {}
        for _ in range(count):
            at = self.pos
            key = self.value(depth + 1)
            if isinstance(key, (list, dict)):
                raise MsgpackError(f"a map key at byte {at} is a container")
            out[key] = self.value(depth + 1)
        return out


def unpack(buf):
    """The one MessagePack value `buf` holds. Raises `MsgpackError` for bytes that aren't one
    well-formed value within the reader's bounds, trailing bytes included."""
    reader = _Reader(buf)
    if not reader.buf:
        raise MsgpackError("an empty body holds no value")
    value = reader.value(0)
    if reader.pos != len(reader.buf):
        raise MsgpackError(f"{len(reader.buf) - reader.pos} byte(s) after the value at byte "
                           f"{reader.pos}")
    return value


def _uint(n, sizes=((0xCC, 1), (0xCD, 2), (0xCE, 4), (0xCF, 8))):
    for tag, size in sizes:
        if n < 1 << (8 * size):
            return bytes([tag]) + n.to_bytes(size, "big")
    raise ValueError(f"{n} is past 64 bits")


def pack(value):
    """`value` as MessagePack, for fixtures: the smallest form of each int and length, a float
    as `float 64`, text as `str`, bytes as `bin`, a list as an array, a dict as a map in its
    order, and `Ext` as an `ext`."""
    if value is None:
        return b"\xc0"
    if isinstance(value, bool):
        return b"\xc3" if value else b"\xc2"
    if isinstance(value, int):
        if 0 <= value <= 0x7F:
            return bytes([value])
        if -32 <= value < 0:
            return bytes([value + 0x100])
        if value >= 0:
            return _uint(value)
        for tag, size in ((0xD0, 1), (0xD1, 2), (0xD2, 4), (0xD3, 8)):
            if -(1 << (8 * size - 1)) <= value:
                return bytes([tag]) + value.to_bytes(size, "big", signed=True)
        raise ValueError(f"{value} is past 64 bits")
    if isinstance(value, float):
        return b"\xcb" + struct.pack(">d", value)
    if isinstance(value, str):
        raw = value.encode()
        if len(raw) < 32:
            return bytes([0xA0 | len(raw)]) + raw
        return _sized(raw, ((0xD9, 1), (0xDA, 2), (0xDB, 4)))
    if isinstance(value, (bytes, bytearray)):
        return _sized(bytes(value), ((0xC4, 1), (0xC5, 2), (0xC6, 4)))
    if isinstance(value, Ext):
        return (_sized(b"", ((0xC7, 1), (0xC8, 2), (0xC9, 4)), len(value.data))
                + value.code.to_bytes(1, "big", signed=True) + value.data)
    if isinstance(value, list):
        head = (bytes([0x90 | len(value)]) if len(value) < 16
                else _sized(b"", ((0xDC, 2), (0xDD, 4)), len(value)))
        return head + b"".join(pack(item) for item in value)
    if isinstance(value, dict):
        head = (bytes([0x80 | len(value)]) if len(value) < 16
                else _sized(b"", ((0xDE, 2), (0xDF, 4)), len(value)))
        return head + b"".join(pack(k) + pack(v) for k, v in value.items())
    raise TypeError(f"no MessagePack form for {type(value).__name__}")


def _sized(raw, forms, count=None):
    count = len(raw) if count is None else count
    for tag, size in forms:
        if count < 1 << (8 * size):
            return bytes([tag]) + count.to_bytes(size, "big") + raw
    raise ValueError(f"{count} is past 32 bits")
