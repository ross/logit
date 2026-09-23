#!/usr/bin/env python3
"""Sends one length-prefixed carbon pickle frame to `raw_capture.py --proto tcp` for
`script/record-fixtures`'s `graphite` producer -- stdlib only (`pickle`, `struct`, `socket`), no
carbon/graphite/Twisted dependency, so this exercises exactly the wire format
`crates/logit-proto/src/graphite/pickle.rs`'s restricted reader has to accept: a 4-byte
big-endian length prefix (Twisted's `Int32StringReceiver` framing carbon's own pickle receiver
inherits), followed by `pickle.dumps([(path, (timestamp, value)), ...], protocol=...)`.

Run twice by `record_graphite()` in `script/record-fixtures` -- once with `--protocol 2` and once
with `--protocol -1` (Python's "highest available" sentinel, which resolves to protocol 5 on
`python:3.12-slim`, the image every producer in this file runs in) -- against two separate
`raw_capture.py` listeners, so each protocol lands in its own fixture file
(`graphite-pickle-p2-000.raw` / `graphite-pickle-p5-000.raw`) rather than two frames sharing one.
`crates/logit-inputs/src/graphite/mod.rs`'s `interop_fixture_pickle_protocol_2_decodes` and
`interop_fixture_pickle_protocol_5_decodes` each read one of those files.

Protocol -1 is the one a real sender reaching for "the best available" would use, and is what
makes this exercise `FRAME` (0x95, a protocol-4+ opcode wrapping the whole payload),
`SHORT_BINUNICODE` (0x8c, protocol 4+'s compact string opcode for short strings -- protocol 2 uses
plain `BINUNICODE` instead) and `MEMOIZE` (0x94, protocol 4+'s single-opcode memo store) -- three
opcodes protocol 2 never emits, all three on the restricted reader's allow-list
(`docs/adr/graphite-carbon-relay.md`'s "Pickle opcode subset", and
`crates/logit-proto/src/graphite/pickle.rs`'s "Accepted opcodes").

Both runs pickle the exact same `DATAPOINTS` below: a handful of `(path, (timestamp, value))`
tuples with a deliberate mix of `int`/`float` timestamps and `int`/`float` values (pickle encodes
those differently -- an `int` becomes `BININT1`/`BININT` here (`LONG1` only past `i32`, which none
of these reach), a `float` always `BINFLOAT`), so the two
captured fixtures carry identical decoded events and a consuming test can assert the exact same
paths/values against either one. Every path is prefixed `logit-fixture.`, matching the
`write_graphite` fixture's collectd `Hostname`, so both `graphite` producers' fixtures satisfy the
same "decoded paths start with logit-fixture." assertion.

Usage: python3 python_graphite_pickle_producer.py --host capture --port 2004 --protocol 2
"""

import argparse
import pickle
import socket
import struct
import sys

# Fixed, deterministic datapoints -- documented here because the consuming Rust tests
# (`crates/logit-inputs/src/graphite/mod.rs`'s `interop_fixture_pickle_protocol_{2,5}_decodes`)
# assert on these exact paths and values. Do not change without updating those tests.
DATAPOINTS = [
    ("logit-fixture.pickle.int_value", (1700000000, 42)),
    ("logit-fixture.pickle.float_value", (1700000001.5, 12.75)),
    ("logit-fixture.pickle.negative_value", (1700000002, -17.5)),
    ("logit-fixture.pickle.large_value", (1700000003, 1234567.0)),
]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", required=True, help="Hostname/container name of the raw_capture.py listener")
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--protocol", type=int, required=True, help="pickle protocol, e.g. 2 or -1 (highest available)")
    args = ap.parse_args()

    # Provenance for testdata/interop/graphite/README.md: -1 is a moving target across Python
    # versions (it has meant protocol 4 as recently as Python 3.7), so the table records what it
    # actually resolved to in the container that did the recording, not just the literal `-1` this
    # script was invoked with.
    print(
        f"python_graphite_pickle_producer: pickle.HIGHEST_PROTOCOL={pickle.HIGHEST_PROTOCOL} "
        f"(requested protocol={args.protocol})",
        file=sys.stderr,
    )

    payload = pickle.dumps(DATAPOINTS, protocol=args.protocol)
    frame = struct.pack("!I", len(payload)) + payload

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((args.host, args.port))
    sock.sendall(frame)
    sock.close()


if __name__ == "__main__":
    main()
