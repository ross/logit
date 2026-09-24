#!/usr/bin/env python3
"""Sends one length-prefixed carbon pickle frame to `raw_capture.py --proto tcp` for
`script/record-fixtures`'s `graphite` producer. Stdlib only, with no carbon or Twisted dependency:
the frame is a 4-byte big-endian length prefix (Twisted's `Int32StringReceiver` framing, which
carbon's pickle receiver inherits) followed by
`pickle.dumps([(path, (timestamp, value)), ...], protocol=...)`, the wire format
`crates/logit-proto/src/graphite/pickle.rs`'s restricted reader must accept.

`record_graphite()` runs it twice, with `--protocol 2` and `--protocol -1` (highest available: 5
on `python:3.12-slim`), against separate listeners, so each protocol lands in its own fixture
(`graphite-pickle-p2-000.raw`, `graphite-pickle-p5-000.raw`) for
`interop_fixture_pickle_protocol_{2,5}_decodes` in `crates/logit-inputs/src/graphite/mod.rs`.

Protocol -1 is what a sender reaching for the best available uses, and it emits three protocol-4+
opcodes protocol 2 never does, all on the reader's allow-list: `FRAME` (0x95), `SHORT_BINUNICODE`
(0x8c; protocol 2 uses `BINUNICODE`), and `MEMOIZE` (0x94). See
`docs/adr/graphite-carbon-relay.md`'s "Pickle opcode subset" and `pickle.rs`'s "Accepted opcodes".

Both runs pickle the same `DATAPOINTS`, mixing `int` and `float` timestamps and values (an `int`
encodes as `BININT1`/`BININT`, since none passes `i32`; a `float` as `BINFLOAT`), so both fixtures
decode to identical events. Every path starts `logit-fixture.`, matching the `write_graphite`
fixture's collectd `Hostname`, so both producers' fixtures satisfy the same path assertion.

Usage: python3 python_graphite_pickle_producer.py --host capture --port 2004 --protocol 2
"""

import argparse
import pickle
import socket
import struct
import sys

# `interop_fixture_pickle_protocol_{2,5}_decodes` (crates/logit-inputs/src/graphite/mod.rs)
# assert on these exact paths and values; change them together.
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

    # Provenance for testdata/interop/graphite/README.md: -1 resolves differently across Python
    # versions (4 on Python 3.7), so the table records what it resolved to in this container.
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
