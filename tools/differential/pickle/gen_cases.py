"""Writes the carbon pickle differential corpus, `testdata/differential/graphite-pickle/`, for
`script/differential pickle`. Stdlib only; runs on the pinned CPython 3.12 image.

Each case is a pickle payload CPython 3 or Python 2 wrote (or, for a `hand` case, bytes another
producer's shape gives) and a JSON file holding three readings of it:

- `cpython`: this interpreter's `pickle.loads(data)`, as tagged canonical JSON (`tag`), or the
  exception it raised;
- `carbon`: what carbon's pickle receiver does with it, from `carbon_reading`, a transcription
  of carbon 1.1.10's receiver (no carbon or Twisted install);
- `logit`: the verdict `crates/logit-proto/src/graphite/pickle.rs`'s reader must give, declared
  here per case.

A case whose `logit` verdict differs from `carbon` carries a `divergence` naming why, and
`crates/logit-proto/tests/graphite_pickle_differential.rs` checks both directions. The Python 2
payloads come from `gen_py2.py`, run first on the Python 2 image; its output is `--py2`.

Generation fails, writing nothing, when a case's opcodes don't contain what it claims to exercise,
when a rejected case's first unlisted opcode isn't the one it declares, or when the corpus misses
an opcode `pickle.rs`'s module doc lists ("Accepted opcodes" and the rejection paragraph after
it) and isn't in `ACCEPT_UNREACHABLE` or `REJECT_UNREACHABLE`.

Usage: PYTHONHASHSEED=0 python3 -P -s gen_cases.py --py2 py2.json --allowlist pickle.rs --interop <dir> --out <dir>
"""

import argparse
import copyreg
import datetime
import decimal
import io
import json
import os
import pickle
import pickletools
import re
import struct
import sys

# -- carbon's receiver, transcribed ---------------------------------------------------------------
#
# carbon 1.1.10 (tag 1.1.10, commit 6fd9cd2890185195bdc69e65ca022842feaee7cc):
# - `lib/carbon/util.py`'s `SafeUnpickler`, the non-cPickle branch Python 3 takes: an
#   `Unpickler` subclass whose `find_class` allows only `copy_reg._reconstructor` and
#   `__builtin__.object`, loaded with `encoding='utf-8'`. Python 3 has neither module, so its
#   `__import__` raises `ImportError` for both.
# - `lib/carbon/protocols.py`'s `MetricPickleReceiver.stringReceived`: the `except` list a failed
#   load is caught by, the per-item unpack and `float()` coercion, and the `str` check.
# - the same file's `MetricReceiver.metricReceived`, with no blacklist or whitelist and the default
#   `MIN_TIMESTAMP_RESOLUTION = 0` (`lib/carbon/conf.py`): a NaN value is dropped, and a
#   timestamp whose `int()` is `-1` becomes the receipt time.
#
# An exception outside `stringReceived`'s `except` list leaves it, and Twisted closes the
# connection; the reading records it as `raised`, and nothing after it in the frame is received.


class CarbonSafeUnpickler(pickle.Unpickler):
    PICKLE_SAFE = {
        "copy_reg": set(["_reconstructor"]),
        "__builtin__": set(["object"]),
    }

    def find_class(self, module, name):
        if module not in self.PICKLE_SAFE:
            raise pickle.UnpicklingError("Attempting to unpickle unsafe module %s" % module)
        __import__(module)
        mod = sys.modules[module]
        if name not in self.PICKLE_SAFE[module]:
            raise pickle.UnpicklingError("Attempting to unpickle unsafe class %s" % name)
        return getattr(mod, name)


CARBON_CAUGHT = (pickle.UnpicklingError, ValueError, IndexError, ImportError, KeyError, EOFError)


def metric_received(datapoint):
    """`MetricReceiver.metricReceived`'s fate for one datapoint; may raise, as carbon's does."""
    if datapoint[1] != datapoint[1]:
        return "nan"
    if int(datapoint[0]) == -1:
        return "now"
    return "store"


def carbon_reading(data):
    try:
        datapoints = CarbonSafeUnpickler(io.BytesIO(data), encoding="utf-8").load()
    except CARBON_CAUGHT as exc:
        return {"outcome": "dropped", "exception": exception(exc), "received": []}
    except Exception as exc:
        return {"outcome": "raised", "exception": exception(exc), "received": []}
    reading = {"outcome": "ok", "received": []}
    if hasattr(datapoints, "__len__"):
        reading["items"] = len(datapoints)
    received = []
    try:
        for raw in datapoints:
            try:
                (metric, (value, timestamp)) = raw
            except Exception:
                continue
            try:
                datapoint = (float(value), float(timestamp))
            except (ValueError, TypeError):
                continue
            if not isinstance(metric, str):
                metric = metric.encode("utf-8")
            entry = [tag(metric), tag(datapoint[0]), tag(datapoint[1])]
            received.append(entry)
            try:
                entry.append(metric_received(datapoint))
            except Exception as exc:
                entry.append({"raises": type(exc).__name__})
                raise
    except Exception as exc:
        reading["outcome"] = "raised"
        reading["exception"] = exception(exc)
    reading["received"] = rle(received)
    return reading


# -- canonical JSON -------------------------------------------------------------------------------


def f64(x):
    bits = struct.unpack(">Q", struct.pack(">d", x))[0]
    return {"f64": "0x%016x" % bits, "repr": repr(x)}


def rle(items):
    """Runs of four or more equal elements as `{"repeat": n, "of": element}`."""
    out = []
    i = 0
    while i < len(items):
        j = i
        while j < len(items) and items[j] == items[i]:
            j += 1
        if j - i >= 4:
            out.append({"repeat": j - i, "of": items[i]})
        else:
            out.extend(items[i:j])
        i = j
    return out


def tag(x, open_ids=()):
    if id(x) in open_ids:
        return {"cycle": type(x).__name__}
    t = type(x)
    if x is None or t is bool:
        return x
    if t is int:
        return {"int": str(x)}
    if t is float:
        return f64(x)
    if t is str:
        try:
            x.encode("utf-8")
            return x
        except UnicodeEncodeError:
            return {"surrogates": [ord(c) for c in x]}
    if t is bytes:
        return {"bytes": x.hex()}
    if t is bytearray:
        return {"bytearray": x.hex()}
    inner = open_ids + (id(x),)
    if t is tuple:
        return {"tuple": rle([tag(e, inner) for e in x])}
    if t is list:
        return {"list": rle([tag(e, inner) for e in x])}
    if t is dict:
        return {"dict": [[tag(k, inner), tag(v, inner)] for k, v in x.items()]}
    if t in (set, frozenset):
        members = sorted((tag(e, inner) for e in x), key=lambda m: json.dumps(m, sort_keys=True))
        return {t.__name__: members}
    return {"object": "%s.%s" % (t.__module__, t.__qualname__), "repr": repr(x)}


def exception(exc):
    return {"error": type(exc).__name__, "message": str(exc)}


def cpython_reading(data):
    try:
        return tag(pickle.loads(data))
    except Exception as exc:
        return exception(exc)


# -- objects the reject cases pickle --------------------------------------------------------------


class Old:
    """The class `gen_py2.py`'s old-style `Old` instances name (`__main__.Old`)."""

    def __repr__(self):
        return "Old(%r)" % sorted(self.__dict__.items())


class Sample:
    """A new-style instance: `NEWOBJ` and `BUILD` at protocol 2."""

    def __init__(self):
        self.v = 1

    def __repr__(self):
        return "Sample(v=%r)" % self.v


class Keyword:
    """`__getnewargs_ex__` with keyword arguments: `NEWOBJ_EX` at protocol 4."""

    def __new__(cls, v=0):
        obj = super().__new__(cls)
        obj.v = v
        return obj

    def __getnewargs_ex__(self):
        return ((), {"v": self.v})

    def __repr__(self):
        return "Keyword(v=%r)" % self.v


class PersistentPickler(pickle.Pickler):
    """Pickles the string `"persistent"` by persistent id: `BINPERSID`, or `PERSID` at protocol 0."""

    def persistent_id(self, obj):
        if isinstance(obj, str) and obj == "persistent":
            return "pid-1"
        return None


def dumps_persistent(obj, protocol):
    buf = io.BytesIO()
    PersistentPickler(buf, protocol=protocol).dump(obj)
    return buf.getvalue()


def dumps_with_extension(obj, protocol, code):
    """`obj` pickled with `decimal.Decimal` registered as copyreg extension `code` (`EXT1` below
    256, `EXT2` below 65536, `EXT4` past it); unregistered again before anything reads it, as a
    receiver's registry would be."""
    copyreg.add_extension("decimal", "Decimal", code)
    try:
        return pickle.dumps(obj, protocol=protocol)
    finally:
        copyreg.remove_extension("decimal", "Decimal", code)


def dumps_out_of_band(obj):
    """Protocol 5 with an out-of-band buffer, which the reader gets no `buffers=` for."""
    return pickle.dumps(obj, protocol=5, buffer_callback=lambda buffer: False)


# -- cases ----------------------------------------------------------------------------------------

USER = "base.cpu.user"
BASE = [
    (USER, (1700000000, 1.5)),
    ("base.load;host=web-1", (1700000001.25, -2)),
    ("base.mem", (1700000002, 1234567.0)),
    (USER, (1700000003, 0)),
]
TS = 1700000000


def ok(skipped=0, datapoints="carbon"):
    """The reader yields `datapoints` (carbon's, or an explicit `[(path, ts, value)]`) and skips
    `skipped` items."""
    if datapoints != "carbon":
        datapoints = [[tag(p), f64(float(t)), f64(float(v))] for p, t, v in datapoints]
    return {"verdict": "ok", "datapoints": datapoints, "skipped": skipped}


def malformed(opcode=None, message=None):
    """The reader fails the frame: on a disallowed `opcode`, or with an error containing `message`."""
    verdict = {"verdict": "malformed"}
    if opcode is not None:
        verdict["opcode"] = "0x%02x" % opcode
    else:
        verdict["message"] = message
    return verdict


CASES = []


def case(name, *, data, protocol, producer, exercises, logit, description, divergence=None,
         decoder_skips=None, decoder_divergence=None):
    entry = {
        "case": name,
        "description": description,
        "producer": producer,
        "protocol": protocol,
        "exercises": list(exercises),
        "logit": logit,
    }
    if divergence:
        entry["divergence"] = divergence
    if decoder_skips:
        entry["decoder_skips"] = decoder_skips
    if decoder_divergence:
        entry["decoder_divergence"] = decoder_divergence
    CASES.append((name, data, entry))


def py3(name, obj, protocol, exercises, logit, description, **kw):
    case(name, data=pickle.dumps(obj, protocol=protocol), protocol=protocol, producer="cpython3",
         exercises=exercises, logit=logit, description=description, **kw)


def one(path, ts, value):
    return [(path, (ts, value))]


# Divergence reasons, each pointing at the `docs/known-gaps/mappings.md` row that records it.
GAP_COERCE = ("carbon's float() reads this value; the reader reads only numbers and numeric "
              "strings Rust's f64 parse takes (docs/known-gaps/mappings.md: decode (Graphite), a "
              "value carbon's float() coerces)")
GAP_SURROGATE = ("carbon keeps a str with a lone surrogate; a logit name is UTF-8, so the reader "
                 "fails the frame (docs/known-gaps/mappings.md: decode (Graphite), a lone surrogate)")
GAP_CONTAINER = ("carbon iterates any container and float()s any value it can; the reader takes "
                 "a list batch and has no opcode for this shape (docs/known-gaps/mappings.md: "
                 "decode (Graphite), a shape carbon iterates)")
GAP_TUPLE_BATCH = ("carbon iterates any container; the reader takes only a list at STOP "
                   "(docs/known-gaps/mappings.md: decode (Graphite), a shape carbon iterates)")
GAP_LIST_ITEM = ("carbon unpacks any two-element sequence; the reader fails a non-empty list item "
                 "in a list grown by APPEND/APPENDS (docs/known-gaps/mappings.md: decode "
                 "(Graphite), a list-shaped pickle datapoint)")
KEEPS_BYTES_PATH = ("carbon calls .encode on a path that isn't a str, which raises on Python 3 "
                    "bytes and closes the connection; the reader reads bytes as UTF-8 text, as "
                    "carbon on Python 2 did (docs/known-gaps/mappings.md: decode (Graphite), "
                    "a frame carbon fails that the reader keeps)")
KEEPS_BAD_TS = ("carbon's metricReceived calls int() on the timestamp, which raises on NaN and "
                "infinity and closes the connection; the reader yields the datapoint, and the "
                "decoder counts it bad_timestamp (docs/known-gaps/mappings.md: decode (Graphite), "
                "a frame carbon fails that the reader keeps)")
DEC_NON_POSITIVE = ("carbon stores a timestamp at or below zero, which whisper then discards as "
                    "outside every retention; the decoder counts it bad_timestamp "
                    "(docs/known-gaps/mappings.md: decode (Graphite), a timestamp carbon reads "
                    "differently)")
DEC_SENTINEL = ("carbon's sentinel test is int(timestamp) == -1, so it reads -1.5 as the receipt "
                "time; the decoder compares the value to -1.0 and counts bad_timestamp "
                "(docs/known-gaps/mappings.md: decode (Graphite), a timestamp carbon reads "
                "differently)")
DEC_INFINITE = ("carbon drops only a NaN value and stores infinity; the decoder skips every "
                "non-finite value (docs/known-gaps/mappings.md: encode (Graphite), a non-finite "
                "value)")


def py3_cases():
    # Protocols, over one base batch.
    for protocol, ops in [
        (0, ["MARK", "LIST", "PUT", "UNICODE", "INT", "FLOAT", "TUPLE", "APPEND", "GET"]),
        (1, ["MARK", "EMPTY_LIST", "BINPUT", "BINUNICODE", "BININT", "BINFLOAT", "TUPLE",
             "APPENDS", "BINGET", "BININT1"]),
        (2, ["PROTO", "EMPTY_LIST", "BINPUT", "BINUNICODE", "TUPLE2", "APPENDS", "BINGET"]),
        (3, ["PROTO", "BINUNICODE", "TUPLE2"]),
        (4, ["PROTO", "FRAME", "SHORT_BINUNICODE", "MEMOIZE", "TUPLE2"]),
        (5, ["PROTO", "FRAME", "SHORT_BINUNICODE", "MEMOIZE"]),
    ]:
        py3("py3-base-p%d" % protocol, BASE, protocol, ops, ok(),
            "The base batch at protocol %d: int and float timestamps and values, a tagged path, "
            "and one path object used twice." % protocol, decoder_skips=None)

    # Paths.
    multibyte = one("path.caf\u00e9.\u20ac.\U0001f600", TS, 1.0)
    py3("py3-path-utf8-p2", multibyte, 2, ["BINUNICODE"], ok(),
        "A path with two-, three-, and four-byte UTF-8 characters.")
    py3("py3-path-utf8-p0", multibyte, 0, ["UNICODE"], ok(),
        "The same path in protocol 0's raw-unicode-escape: a raw Latin-1 byte, a \\u escape, "
        "and a \\U escape.")
    surrogate = one("path.\udc80", TS, 1.0)
    py3("py3-path-surrogate-p2", surrogate, 2, ["BINUNICODE"],
        malformed(message="not valid utf-8"),
        "A str holding a lone surrogate, which CPython writes with surrogatepass.",
        divergence=GAP_SURROGATE)
    py3("py3-path-surrogate-p0", surrogate, 0, ["UNICODE"],
        malformed(message="is a surrogate"),
        "The lone surrogate as protocol 0's \\udc80 escape.", divergence=GAP_SURROGATE)
    py3("py3-path-bytes-p3", one(b"path.bytes", TS, 1.0), 3, ["SHORT_BINBYTES"],
        ok(datapoints=[("path.bytes", TS, 1.0)]),
        "A bytes path at protocol 3, carbon's Python 2 str.", divergence=KEEPS_BYTES_PATH)
    py3("py3-path-bytes-long-p3", one(b"path.bytes." + b"x" * 300, TS, 1.0), 3, ["BINBYTES"],
        ok(datapoints=[("path.bytes." + "x" * 300, TS, 1.0)]),
        "A bytes path longer than 255 bytes, which protocol 3 writes as BINBYTES.",
        divergence=KEEPS_BYTES_PATH)
    py3("py3-path-bytes-p2", one(b"path.bytes", TS, 1.0), 2, ["GLOBAL", "REDUCE"],
        malformed(opcode=0x63),
        "A bytes path at protocol 2, which CPython writes as _codecs.encode through GLOBAL and "
        "REDUCE.")

    # Timestamps: every integer opcode CPython picks, floats, and numeric strings.
    for name, ts, ops, extra in [
        ("binint1", 200, ["BININT1"], {}),
        ("binint2", 60000, ["BININT2"], {}),
        ("binint", TS, ["BININT"], {}),
        ("float", 1700000000.5, ["BINFLOAT"], {}),
        ("2p31", 2 ** 31, ["LONG1"], {}),
        ("2p62", 2 ** 62, ["LONG1"], {}),
        ("string", "1700000000", ["BINUNICODE"], {}),
        ("string-float", "1700000000.25", ["BINUNICODE"], {}),
        ("sentinel-int", -1, ["BININT"], {}),
        ("sentinel-float", -1.0, ["BINFLOAT"], {}),
        ("sentinel-string", "-1", ["BINUNICODE"], {}),
        ("sentinel-fraction", -1.5, ["BINFLOAT"],
         {"decoder_skips": {"bad_timestamp": 1}, "decoder_divergence": DEC_SENTINEL}),
        ("zero", 0, ["BININT1"],
         {"decoder_skips": {"bad_timestamp": 1}, "decoder_divergence": DEC_NON_POSITIVE}),
        ("negative", -5, ["BININT"],
         {"decoder_skips": {"bad_timestamp": 1}, "decoder_divergence": DEC_NON_POSITIVE}),
    ]:
        py3("py3-ts-%s-p2" % name, one("ts." + name, ts, 1.0), 2, ops, ok(),
            "The timestamp %r." % (ts,), **extra)
    for name, ts in [("nan", float("nan")), ("inf", float("inf"))]:
        py3("py3-ts-%s-p2" % name, one("ts." + name, ts, 1.0), 2, ["BINFLOAT"],
            ok(datapoints=[("ts." + name, ts, 1.0)]),
            "The timestamp %r." % ts, divergence=KEEPS_BAD_TS,
            decoder_skips={"bad_timestamp": 1})
    py3("py3-ts-string-infinity-p2", one("ts.infinity", "infinity", 1.0), 2, ["BINUNICODE"],
        ok(datapoints=[("ts.infinity", float("inf"), 1.0)]),
        "The timestamp as the string 'infinity', which both float() and Rust's f64 parse read.",
        divergence=KEEPS_BAD_TS, decoder_skips={"bad_timestamp": 1})
    for name, ts in [("padded", " 1700000000 "), ("underscores", "1_700_000_000"),
                     ("fullwidth", "\uff11\uff17\uff10\uff10\uff10\uff10\uff10\uff10\uff10\uff10")]:
        py3("py3-ts-string-%s-p2" % name, one("ts." + name, ts, 1.0), 2, ["BINUNICODE"],
            ok(skipped=1, datapoints=[]),
            "The timestamp as the string %r, which Python's float() reads." % ts,
            divergence=GAP_COERCE)
    py3("py3-ts-bool-p2", one("ts.bool", True, 1.0), 2, ["NEWTRUE"],
        ok(skipped=1, datapoints=[]), "A True timestamp, which float() reads as 1.0.",
        divergence=GAP_COERCE)
    py3("py3-ts-int-p0", one("ts.int", TS, 1.0), 0, ["INT"], ok(),
        "Protocol 0's INT timestamp.")
    py3("py3-ts-long-p0", one("ts.long", 2 ** 40, 1.0), 0, ["LONG"], ok(),
        "Protocol 0's LONG, which CPython writes for an int past i32.")
    py3("py3-ts-long-p1", one("ts.long", 2 ** 40, 1.0), 1, ["LONG"], ok(),
        "Protocol 1 has no LONG1, so it writes the text LONG too.")

    # Values.
    for name, value, ops, extra in [
        ("nan", float("nan"), ["BINFLOAT"], {"decoder_skips": {"non_finite_value": 1}}),
        ("inf", float("inf"), ["BINFLOAT"],
         {"decoder_skips": {"non_finite_value": 1}, "decoder_divergence": DEC_INFINITE}),
        ("neg-inf", float("-inf"), ["BINFLOAT"],
         {"decoder_skips": {"non_finite_value": 1}, "decoder_divergence": DEC_INFINITE}),
        ("neg-zero", -0.0, ["BINFLOAT"], {}),
        ("subnormal", 5e-324, ["BINFLOAT"], {}),
        ("max", sys.float_info.max, ["BINFLOAT"], {}),
        ("2p53-plus-1", 2 ** 53 + 1, ["LONG1"], {}),
        ("2p63-minus-1", 2 ** 63 - 1, ["LONG1"], {}),
        ("neg-2p63", -(2 ** 63), ["LONG1"], {}),
        ("2p63", 2 ** 63, ["LONG1"], {}),
        ("2p64-minus-1", 2 ** 64 - 1, ["LONG1"], {}),
        ("neg-2p63-minus-1", -(2 ** 63) - 1, ["LONG1"], {}),
        ("2p127-minus-1", 2 ** 127 - 1, ["LONG1"], {}),
        ("string", "3.25", ["BINUNICODE"], {}),
        ("string-int", "42", ["BINUNICODE"], {}),
        ("string-nan", "NaN", ["BINUNICODE"], {"decoder_skips": {"non_finite_value": 1}}),
        ("string-exponent", "1e5", ["BINUNICODE"], {}),
        ("string-overflow", "1e400", ["BINUNICODE"],
         {"decoder_skips": {"non_finite_value": 1}, "decoder_divergence": DEC_INFINITE}),
        ("bytes", b"2.5", ["SHORT_BINBYTES"], {}),
    ]:
        protocol = 3 if isinstance(value, bytes) else 2
        py3("py3-value-%s-p%d" % (name, protocol), one("value." + name, TS, value), protocol,
            ops, ok(), "The value %r." % (value,), **extra)
    py3("py3-value-2p127-p2", one("value.2p127", TS, 2 ** 127), 2, ["LONG1"],
        malformed(message="magnitude cap"),
        "2**127, a 17-byte LONG1, past the reader's i128 bound.",
        divergence=("carbon's float() reads an int up to about 1.8e308; the reader reads one "
                    "that fits an i128 (docs/known-gaps/mappings.md: decode (Graphite), a value "
                    "carbon's float() coerces)"))
    py3("py3-value-huge-p2", one("value.huge", TS, 10 ** 700), 2, ["LONG4"],
        malformed(message="magnitude cap"),
        "10**700, a LONG4 past f64's range: carbon's float() raises OverflowError.")
    py3("py3-value-huge-p0", one("value.huge", TS, 10 ** 700), 0, ["LONG"],
        malformed(message="does not fit"),
        "10**700 as protocol 0's LONG.")
    py3("py3-value-long-p0", [("value.2p63", (TS, 2 ** 63)), ("value.neg", (TS, -(2 ** 63) - 1))],
        0, ["LONG"], ok(), "Ints past i64 in protocol 0's LONG.")
    for name, value in [("padded", " 1.5 "), ("underscores", "1_000"),
                        ("fullwidth", "\uff11\uff12"), ("hex", "0x10")]:
        hex_value = name == "hex"
        py3("py3-value-string-%s-p2" % name, one("value." + name, TS, value), 2, ["BINUNICODE"],
            ok(skipped=1, datapoints=[] if not hex_value else "carbon"),
            "The value as the string %r." % value,
            divergence=None if hex_value else GAP_COERCE)
    for name, value, op in [("true", True, "NEWTRUE"), ("false", False, "NEWFALSE")]:
        py3("py3-value-%s-p2" % name, one("value." + name, TS, value), 2, [op],
            ok(skipped=1, datapoints=[]),
            "The value %r, which float() reads as %r." % (value, float(value)),
            divergence=GAP_COERCE)
    py3("py3-value-none-p2", one("value.none", TS, None), 2, ["NONE"], ok(skipped=1),
        "A None value, which float() refuses.")
    py3("py3-value-floats-p0",
        [("p0.nan", (TS, float("nan"))), ("p0.inf", (TS, float("inf"))),
         ("p0.neg-zero", (TS, -0.0)), ("p0.subnormal", (TS, 5e-324)),
         ("p0.max", (TS, sys.float_info.max)), ("p0.bool", (TS, True))],
        0, ["FLOAT", "INT"], ok(skipped=1, datapoints=[
            ("p0.nan", TS, float("nan")), ("p0.inf", TS, float("inf")), ("p0.neg-zero", TS, -0.0),
            ("p0.subnormal", TS, 5e-324), ("p0.max", TS, sys.float_info.max)]),
        "Protocol 0's FLOAT spellings of nan, inf, -0.0, the smallest subnormal, and f64's "
        "maximum, and I01 for True.", divergence=GAP_COERCE,
        decoder_skips={"non_finite_value": 2})

    # Structure.
    py3("py3-empty-p2", [], 2, ["EMPTY_LIST"], ok(), "An empty batch.")
    py3("py3-empty-p0", [], 0, ["LIST"], ok(), "An empty batch in protocol 0.")
    paths = ["memo.%03d" % i for i in range(90)]
    big_memo = [(p, (TS + i, float(i))) for i, p in enumerate(paths)] + [(paths[-1], (TS, 0.5))]
    py3("py3-memo-long-p2", big_memo, 2, ["LONG_BINPUT", "LONG_BINGET"], ok(),
        "Ninety datapoints memoize 270 objects, so the memo passes key 255, and the last path "
        "repeats through LONG_BINGET.")
    shared = ("frame.shared", (TS, 1.0))
    py3("py3-frames-p4", [shared] * 34000, 4, ["FRAME", "BINGET", "APPENDS"], ok(),
        "One datapoint object 34,000 times: about 68 KB, so the pickler writes two FRAMEs.")
    py3("py3-nested-list-p2", [[("nested.a", (TS, 1.0))]], 2, ["EMPTY_LIST", "APPEND"],
        malformed(message="cannot extend"), "A batch whose one item is itself a list.",
        divergence=GAP_LIST_ITEM)
    py3("py3-list-shaped-p2", [["list.a", [TS, 1.0]], ["list.b", [TS, 2.0]]], 2, ["APPENDS"],
        malformed(message="cannot extend"),
        "List-shaped datapoints, [path, [timestamp, value]].", divergence=GAP_LIST_ITEM)
    py3("py3-list-inner-p2", [("inner.a", [TS, 1.0])], 2, ["TUPLE2", "APPEND"],
        malformed(message="cannot extend"),
        "A tuple datapoint whose (timestamp, value) is a list.", divergence=GAP_LIST_ITEM)
    py3("py3-tuple-batch-p2", (("tuple.a", (TS, 1.0)),), 2, ["TUPLE1"],
        malformed(message="not a list"), "A batch that is a tuple.", divergence=GAP_TUPLE_BATCH)
    py3("py3-tuple-shapes-p2",
        [("shape.ok", (TS, 1.0)), ("shape.one",), ("shape.three", (TS, 1.0), 3),
         ("shape.inner3", (TS, 1.0, 2.0)), (), ("shape.big", (TS, 1.0), 3, 4)],
        2, ["TUPLE1", "TUPLE3", "EMPTY_TUPLE", "TUPLE"], ok(skipped=5),
        "Wrong-sized tuples: 1-, 3-, and 4-tuple items, an empty one, and a 3-tuple inner pair; "
        "carbon's unpack refuses each.")

    # Rejects: containers, objects, and the opcodes that import or call.
    item = ("reject.a", (TS, 1.0))
    py3("py3-dict-batch-p2", {"reject.a": (TS, 1.0), "reject.b": (TS, 2.0)}, 2,
        ["EMPTY_DICT", "SETITEMS"], malformed(opcode=0x7d), "A dict batch.",
        divergence=GAP_CONTAINER)
    py3("py3-dict-one-p2", {"reject.a": (TS, 1.0)}, 2, ["EMPTY_DICT", "SETITEM"],
        malformed(opcode=0x7d), "A one-entry dict batch, which CPython writes with SETITEM.",
        divergence=GAP_CONTAINER)
    py3("py3-dict-batch-p0", {"reject.a": (TS, 1.0)}, 0, ["DICT", "SETITEM"],
        malformed(opcode=0x64), "A dict batch in protocol 0.", divergence=GAP_CONTAINER)
    py3("py3-set-batch-p4", {item}, 4, ["EMPTY_SET", "ADDITEMS"], malformed(opcode=0x8f),
        "A set batch at protocol 4, which needs no global.", divergence=GAP_CONTAINER)
    py3("py3-frozenset-batch-p4", frozenset([item]), 4, ["FROZENSET"], malformed(opcode=0x91),
        "A frozenset batch at protocol 4: MARK, the items, FROZENSET.",
        divergence=GAP_CONTAINER)
    py3("py3-set-batch-p2", {item}, 2, ["GLOBAL", "REDUCE"], malformed(opcode=0x63),
        "A set batch at protocol 2, which CPython writes as builtins.set through GLOBAL.")
    py3("py3-value-decimal-p2", one("reject.decimal", TS, decimal.Decimal("1.5")), 2,
        ["GLOBAL", "REDUCE"], malformed(opcode=0x63), "A decimal.Decimal value.")
    py3("py3-value-decimal-p4", one("reject.decimal", TS, decimal.Decimal("1.5")), 4,
        ["STACK_GLOBAL", "REDUCE"], malformed(opcode=0x93),
        "A decimal.Decimal value at protocol 4, through STACK_GLOBAL.")
    py3("py3-value-datetime-p2",
        one("reject.datetime", TS, datetime.datetime(2023, 11, 14, 22, 13, 20)), 2,
        ["GLOBAL", "REDUCE"], malformed(opcode=0x63), "A datetime.datetime value.")
    py3("py3-value-bytearray-p5", one("reject.bytearray", TS, bytearray(b"1.5")), 5,
        ["BYTEARRAY8"], malformed(opcode=0x96), "A bytearray value at protocol 5.",
        divergence=GAP_CONTAINER)
    case("py3-value-picklebuffer-p5",
         data=dumps_out_of_band(one("reject.buffer", TS, pickle.PickleBuffer(b"1.5"))),
         protocol=5, producer="cpython3", exercises=["NEXT_BUFFER", "READONLY_BUFFER"],
         logit=malformed(opcode=0x97),
         description="A read-only PickleBuffer value written out of band.")
    case("py3-persistent-id-p2", data=dumps_persistent(one("persistent", TS, 1.0), 2), protocol=2,
         producer="cpython3", exercises=["BINPERSID"], logit=malformed(opcode=0x51),
         description="A path the pickler writes by persistent id.")
    case("py3-persistent-id-p0", data=dumps_persistent(one("persistent", TS, 1.0), 0), protocol=0,
         producer="cpython3", exercises=["PERSID"], logit=malformed(opcode=0x50),
         description="A persistent id in protocol 0.")
    for op, code, opcode in [("EXT1", 200, 0x82), ("EXT2", 300, 0x83), ("EXT4", 70000, 0x84)]:
        case("py3-extension-%s-p2" % op.lower(),
             data=dumps_with_extension(one("reject.ext", TS, decimal.Decimal), 2, code),
             protocol=2, producer="cpython3", exercises=[op], logit=malformed(opcode=opcode),
             description="The decimal.Decimal class as copyreg extension code %d." % code)
    loop = []
    # Four elements: CPython closes a recursive tuple of up to three with POP, and a longer one
    # with POP_MARK in a binary protocol.
    recursive = (loop, 1, 2, 3)
    loop.append(recursive)
    py3("py3-recursive-tuple-p2", [item, recursive], 2, ["POP_MARK"], malformed(opcode=0x31),
        "A tuple that contains itself through a list.", divergence=GAP_CONTAINER)
    py3("py3-recursive-tuple-p0", [item, recursive], 0, ["POP"], malformed(opcode=0x30),
        "The recursive tuple in protocol 0.", divergence=GAP_CONTAINER)
    py3("py3-value-object-p2", one("reject.object", TS, Sample()), 2,
        ["GLOBAL", "NEWOBJ", "BUILD"], malformed(opcode=0x63), "An instance of a class.")
    py3("py3-value-object-kw-p4", one("reject.object", TS, Keyword(3)), 4,
        ["STACK_GLOBAL", "NEWOBJ_EX"], malformed(opcode=0x93),
        "An instance whose __getnewargs_ex__ passes keyword arguments.")

    # Another producer's shape: og-rek (carbon-relay-ng) builds every list with MARK ... LIST.
    # `X` BINUNICODE, `J` BININT, `G` BINFLOAT, `(` MARK, `l` LIST, `\x86` TUPLE2.
    def unicode(s):
        b = s.encode()
        return b"X" + struct.pack("<I", len(b)) + b

    ts = b"J" + struct.pack("<i", TS)
    one_f = b"G" + struct.pack(">d", 1.0)
    shaped = (b"\x80\x02(" + unicode("mark.list.a") + ts + one_f + b"\x86\x86"
              + b"(" + unicode("mark.list.b") + b"(" + ts + one_f + b"ll" + b"l.")
    case("hand-mark-list-p2", data=shaped, protocol=2, producer="hand",
         exercises=["MARK", "LIST"],
         logit=ok(skipped=1, datapoints=[("mark.list.a", TS, 1.0)]),
         description="og-rek's MARK ... LIST lists, the second item a list-shaped datapoint, "
                     "[path, [timestamp, value]].",
         divergence=("carbon unpacks any two-element sequence, so it receives the list-shaped "
                     "datapoint; inside a MARK ... LIST list the reader skips it "
                     "(docs/known-gaps/mappings.md: decode (Graphite), a list-shaped pickle "
                     "datapoint)"))


def py2_cases(py2):
    def take(name, exercises, logit, description, **kw):
        entry = py2.pop(name)
        case(name, data=bytes.fromhex(entry["hex"]), protocol=entry["protocol"],
             producer="python2", exercises=exercises, logit=logit, description=description, **kw)

    take("py2-base-p0", ["STRING", "PUT", "GET", "INT", "FLOAT", "TUPLE", "APPEND"], ok(),
         "The base batch through cPickle at protocol 0, Diamond's call: a memo from 1.")
    take("py2-base-p1", ["SHORT_BINSTRING", "BINPUT", "BINGET", "APPENDS"], ok(),
         "The base batch at protocol 1.")
    take("py2-base-p2", ["PROTO", "SHORT_BINSTRING", "TUPLE2"], ok(),
         "The base batch at protocol 2, carbon's own client's call.")
    take("py2-path-utf8-str-p0", ["STRING"], ok(), "A UTF-8 str path, escaped as \\xc3\\xa9.")
    take("py2-path-utf8-str-p2", ["SHORT_BINSTRING"], ok(), "A UTF-8 str path at protocol 2.")
    take("py2-path-latin1-str-p0", ["STRING"], malformed(message="not valid utf-8"),
         "A Latin-1 str path, which carbon's utf-8 unpickler refuses too.")
    take("py2-path-latin1-str-p2", ["SHORT_BINSTRING"], malformed(message="not valid utf-8"),
         "A Latin-1 str path at protocol 2.")
    take("py2-path-unicode-p0", ["UNICODE"], ok(),
         "A unicode path: a raw Latin-1 byte and a \\u escape.")
    take("py2-path-unicode-p2", ["BINUNICODE"], ok(), "A unicode path at protocol 2.")
    take("py2-path-long-str-p1", ["BINSTRING"], ok(),
         "A str path past 255 bytes, which protocol 1 writes as BINSTRING.")
    take("py2-path-long-str-p2", ["BINSTRING"], ok(), "The long str path at protocol 2.")
    take("py2-ts-long-p0", ["LONG"], ok(), "A long timestamp: LONG, with its trailing L.")
    take("py2-ts-long-p2", ["LONG1"], ok(), "A long timestamp at protocol 2: LONG1.")
    take("py2-value-int64-p2", ["INT"], ok(),
         "A 64-bit int value past i32: the text INT opcode inside a protocol-2 pickle.")
    take("py2-value-bool-p0", ["INT"], ok(skipped=1, datapoints=[]),
         "A True value: I01.", divergence=GAP_COERCE)
    take("py2-value-oldstyle-p0", ["INST", "BUILD"], malformed(opcode=0x69),
         "An old-style class instance: INST.")
    take("py2-value-oldstyle-p1", ["OBJ", "GLOBAL"], malformed(opcode=0x63),
         "An old-style class instance at protocol 1: GLOBAL, then OBJ.")
    if py2:
        raise SystemExit("gen_cases: gen_py2.py wrote cases this script doesn't declare: %s"
                         % sorted(py2))


# The three recorded captures in `testdata/interop/graphite/` are read in place: one `.json` per
# capture holds the readings of its one length-prefixed frame, and the `.raw` stays where it is.
INTEROP = [
    ("graphite-pickle-p0-000", ok(), None),
    ("graphite-pickle-p2-000", ok(), None),
    ("graphite-pickle-p5-000", ok(), None),
    ("graphite-pickle-py2-000", ok(), None),
    ("graphite-dropwizard-000", ok(), {"non_finite_value": 1}),
]


# -- the allowlist, read from pickle.rs's module doc ----------------------------------------------

# Accepted opcodes no CPython case can contain, each with why.
ACCEPT_UNREACHABLE = {
    "BINUNICODE8": "CPython writes it only for a str of 4 GiB or more",
    "BINBYTES8": "CPython writes it only for bytes of 4 GiB or more",
    "LONG4": "CPython writes it only for an int of 256 bytes or more, past the magnitude the "
             "reader takes, so it appears only in a malformed case",
}
# Rejected opcodes no CPython case can contain, each with why.
REJECT_UNREACHABLE = {
    "DUP": "no CPython or Python 2 pickler writes it",
}

OPCODES_BY_CODE = {op.code.encode("latin-1")[0]: op.name for op in pickletools.opcodes}


def allowlist(path):
    """(accepted, rejected) opcode names from `pickle.rs`'s module doc."""
    with open(path, encoding="utf-8") as f:
        doc = [line[4:] if line.startswith("//! ") else line[3:]
               for line in f.read().splitlines() if line.startswith("//!")]
    text = "\n".join(doc)
    table = text.split("## Accepted opcodes", 1)[1].split("\n## ", 1)[0]
    rows = [line for line in table.splitlines()
            if line.startswith("| ") and not line.startswith("| Group")]
    accepted = {OPCODES_BY_CODE[int(code, 16)]
                for row in rows for code in re.findall(r"`0x([0-9a-f]{2})`", row)}
    paragraph = table.split("Everything else is rejected", 1)[1].split("\n\n", 1)[0]
    rejected = {OPCODES_BY_CODE[int(code, 16)]
                for code in re.findall(r"`0x([0-9a-f]{2})`", paragraph)}
    for name, code in re.findall(r"`([A-Z0-9_]+)` `0x([0-9a-f]{2})`", "\n".join(rows) + paragraph):
        if OPCODES_BY_CODE[int(code, 16)] != name:
            raise SystemExit("gen_cases: pickle.rs names 0x%s %s; pickletools calls it %s"
                             % (code, name, OPCODES_BY_CODE[int(code, 16)]))
    if not accepted or not rejected or accepted & rejected:
        raise SystemExit("gen_cases: couldn't read pickle.rs's opcode lists")
    return accepted, rejected


def opcodes(data):
    return [(op.name, op.code.encode("latin-1")[0]) for op, _, _ in pickletools.genops(data)]


def check(cases, accepted, rejected):
    accept_seen, reject_seen = set(), set()
    for name, data, entry in cases:
        ops = opcodes(data)
        names = {op for op, _ in ops}
        missing = set(entry["exercises"]) - names
        if missing:
            raise SystemExit("gen_cases: %s claims %s but contains none of them"
                             % (name, sorted(missing)))
        first = next(((op, code) for op, code in ops if op not in accepted), None)
        verdict = entry["logit"]
        if verdict["verdict"] == "ok":
            if first:
                raise SystemExit("gen_cases: %s reads, but carries %s" % (name, first[0]))
            accept_seen |= names
        else:
            if "opcode" in verdict and (first is None or verdict["opcode"] != "0x%02x" % first[1]):
                raise SystemExit("gen_cases: %s declares %s, but its first unlisted opcode is %s"
                                 % (name, verdict["opcode"], first))
            if "opcode" not in verdict and first is not None:
                raise SystemExit("gen_cases: %s carries %s; declare it" % (name, first[0]))
            reject_seen |= names & rejected
        if entry["logit"].get("datapoints") == "carbon" and entry["carbon"]["outcome"] != "ok":
            raise SystemExit("gen_cases: %s reads as carbon's, but carbon's outcome is %s"
                             % (name, entry["carbon"]["outcome"]))
    for wanted, seen, unreachable, what in [
        (accepted, accept_seen, ACCEPT_UNREACHABLE, "accepted"),
        (rejected, reject_seen, REJECT_UNREACHABLE, "rejected"),
    ]:
        if not set(unreachable) <= wanted:
            raise SystemExit("gen_cases: unreachable %s opcodes %s aren't in pickle.rs's list"
                             % (what, sorted(set(unreachable) - wanted)))
        if set(unreachable) & seen:
            raise SystemExit("gen_cases: %s opcodes listed unreachable appear in a case: %s"
                             % (what, sorted(set(unreachable) & seen)))
        uncovered = wanted - seen - set(unreachable)
        if uncovered:
            raise SystemExit("gen_cases: no %s case exercises %s" % (what, sorted(uncovered)))


def write_json(path, value):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(value, f, sort_keys=True, indent=1, ensure_ascii=False)
        f.write("\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--py2", required=True)
    ap.add_argument("--allowlist", required=True)
    ap.add_argument("--interop", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    print("gen_cases: Python %s, pickle.HIGHEST_PROTOCOL=%d"
          % (sys.version.split()[0], pickle.HIGHEST_PROTOCOL))
    if sys.flags.hash_randomization:
        raise SystemExit(
            "gen_cases: run with PYTHONHASHSEED=0 and without -E/-I, so a set pickles in one order"
        )

    accepted, rejected = allowlist(args.allowlist)
    with open(args.py2, encoding="utf-8") as f:
        py2 = json.load(f)
    py3_cases()
    py2_cases(py2)
    for name, data, entry in CASES:
        entry["opcodes"] = sorted({op for op, _ in opcodes(data)})
        entry["cpython"] = cpython_reading(data)
        entry["carbon"] = carbon_reading(data)
    names = [name for name, _, _ in CASES]
    if len(set(names)) != len(names):
        raise SystemExit("gen_cases: duplicate case names")
    check(CASES, accepted, rejected)

    interop = []
    for stem, logit, skips in INTEROP:
        with open(os.path.join(args.interop, stem + ".raw"), "rb") as f:
            stream = f.read()
        (length,) = struct.unpack(">I", stream[:4])
        if len(stream) != 4 + length:
            raise SystemExit("gen_cases: %s.raw is not one length-prefixed frame" % stem)
        data = stream[4:]
        entry = {
            "case": "interop-" + stem,
            "description": "testdata/interop/graphite/%s.raw's one frame, its 4-byte big-endian "
                           "length prefix (%d) stripped." % (stem, length),
            "source": "interop/graphite/%s.raw" % stem,
            "length_prefix": length,
            "logit": logit,
            "opcodes": sorted({op for op, _ in opcodes(data)}),
            "cpython": cpython_reading(data),
            "carbon": carbon_reading(data),
        }
        if skips:
            entry["decoder_skips"] = skips
        interop.append(("interop-" + stem, None, entry))

    os.makedirs(args.out, exist_ok=True)
    for name, data, entry in CASES:
        with open(os.path.join(args.out, name + ".pkl"), "wb") as f:
            f.write(data)
        write_json(os.path.join(args.out, name + ".json"), entry)
    for name, _, entry in interop:
        write_json(os.path.join(args.out, name + ".json"), entry)
    divergent = sum(1 for _, _, e in CASES if "divergence" in e)
    print("gen_cases: %d cases (%d divergent from carbon), %d interop readings"
          % (len(CASES), divergent, len(interop)))


if __name__ == "__main__":
    main()
