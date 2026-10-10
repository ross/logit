"""Pickles the Python 2 half of the carbon pickle differential corpus, for `script/differential
pickle`. Python 2 syntax: it runs on `python:2.7.18-slim`, whose `cPickle` is what Python 2
carbon senders (Diamond, graphitesend) call.

Writes one JSON object to the path given as its only argument, mapping each case name to the
protocol it was written at and the payload's bytes as hex. `gen_cases.py` reads that file, adds
CPython 3's and carbon's readings, and writes the corpus; this script decides no verdict.

Every case is `cPickle.dumps(batch, protocol)` on a fixed literal, so a rerun on the same image
writes the same bytes. `gen_cases.py`'s `py2_cases` declares what each name exercises and the
reader's verdict; a name only one of the two scripts knows fails generation.

Usage: python2 gen_py2.py <out.json>
"""

import binascii
import cPickle
import json
import sys

# The same values as gen_cases.py's BASE, as Python 2 `str` paths. USER is one object used twice,
# so its second use is a memo GET.
USER = "base.cpu.user"
BASE = [
    (USER, (1700000000, 1.5)),
    ("base.load;host=web-1", (1700000001.25, -2)),
    ("base.mem", (1700000002, 1234567.0)),
    (USER, (1700000003, 0)),
]


class Old:
    """An old-style class: Python 2 pickles its instance with `INST` at protocol 0 and `OBJ` at
    protocol 1. gen_cases.py defines a class of the same name so CPython 3 can read it."""

    def __init__(self):
        self.v = 1


LONG_PATH = "py2.long." + "x" * 300


def cases():
    out = {}

    def add(name, batch, protocol):
        out[name] = {"protocol": protocol, "hex": binascii.hexlify(cPickle.dumps(batch, protocol))}

    for protocol in (0, 1, 2):
        add("py2-base-p%d" % protocol, BASE, protocol)
    add("py2-path-utf8-str-p0", [("py2.caf\xc3\xa9", (1700000000, 1.0))], 0)
    add("py2-path-utf8-str-p2", [("py2.caf\xc3\xa9", (1700000000, 1.0))], 2)
    add("py2-path-latin1-str-p0", [("py2.caf\xe9", (1700000000, 1.0))], 0)
    add("py2-path-latin1-str-p2", [("py2.caf\xe9", (1700000000, 1.0))], 2)
    add("py2-path-unicode-p0", [(u"py2.caf\xe9.\u20ac", (1700000000, 1.0))], 0)
    add("py2-path-unicode-p2", [(u"py2.caf\xe9.\u20ac", (1700000000, 1.0))], 2)
    add("py2-path-long-str-p1", [(LONG_PATH, (1700000000, 1.0))], 1)
    add("py2-path-long-str-p2", [(LONG_PATH, (1700000000, 1.0))], 2)
    add("py2-ts-long-p0", [("py2.long", (1700000000L, 1.0))], 0)
    add("py2-ts-long-p2", [("py2.long", (1700000000L, 1.0))], 2)
    # A 64-bit Python 2 `int` past i32: cPickle writes the text `INT` opcode even at protocol 2.
    add("py2-value-int64-p2", [("py2.int64", (1700000000, 2 ** 40))], 2)
    add("py2-value-bool-p0", [("py2.flag", (1700000000, True))], 0)
    add("py2-value-oldstyle-p0", [("py2.old", (1700000000, Old()))], 0)
    add("py2-value-oldstyle-p1", [("py2.old", (1700000000, Old()))], 1)
    return out


def main():
    sys.stdout.write("gen_py2: Python %s, pickle.HIGHEST_PROTOCOL=%d\n"
                     % (sys.version.split()[0], cPickle.HIGHEST_PROTOCOL))
    with open(sys.argv[1], "w") as f:
        json.dump(cases(), f, sort_keys=True, indent=1)
        f.write("\n")


if __name__ == "__main__":
    main()
