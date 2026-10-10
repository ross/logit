"""Sends one length-prefixed carbon pickle frame the way Diamond's `GraphitePickleHandler` does on
Python 2, for `script/record-fixtures`'s `graphite` producer. Python 2 syntax: it runs on
`python:2.7-slim`, the last interpreter whose default pickle protocol is 0.

Diamond's handler (`src/diamond/handler/graphitepickle.py`) imports `cPickle as pickle`, batches
`(metric.path, (metric.timestamp, metric.value))` tuples, and sends
`struct.pack("!L", len(payload)) + payload` where `payload = pickle.dumps(self.batch)`, with no
protocol argument. This script makes that same call on a fixed batch, so the capture is the bytes
Diamond writes: protocol 0, a `PUT` memo numbered from 1 (cPickle's numbering, not `pickle`'s),
`STRING` paths in Python 2 `repr` form, and a `GET` when one path object repeats.

`DATAPOINTS` holds a UTF-8 path, which cPickle escapes as `\\xc3\\xa9`, and a `long` timestamp,
written `L1700000003L`, as Diamond's `int` timestamp is on a 32-bit platform.
`interop_fixture_pickle_python_2_decodes` in `crates/logit-inputs/src/graphite/mod.rs` asserts on
these exact paths and values; change them together.

Usage: python2 python2_diamond_pickle_producer.py capture 2004
"""

import cPickle as pickle
import socket
import struct
import sys

USER = "logit-fixture.diamond.cpu.total.user"

DATAPOINTS = [
    (USER, (1700000000, 12.5)),
    ("logit-fixture.diamond.loadavg.01", (1700000000, 0.25)),
    ("logit-fixture.diamond.caf\xc3\xa9.count", (1700000001, 3.0)),
    (USER, (1700000002, -1.0)),
    ("logit-fixture.diamond.uptime", (1700000003L, 7.0)),
]


def main():
    host, port = sys.argv[1], int(sys.argv[2])
    sys.stderr.write("python2_diamond_pickle_producer: %s\n" % sys.version.split()[0])
    payload = pickle.dumps(DATAPOINTS)
    frame = struct.pack("!L", len(payload)) + payload
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))
    sock.sendall(frame)
    sock.close()


if __name__ == "__main__":
    main()
