#!/usr/bin/env python3
"""Builds the key set for murmur2.json and folds Murmur2Vectors.java's output into it.

  murmur2_json.py keys                       prints one hex-encoded key per line
  murmur2_json.py format <partition counts>  reads Murmur2Vectors output on stdin, prints JSON
  murmur2_json.py textkeys                   prints the keys a console tool can carry, as text
  murmur2_json.py compare <java> <kcat-murmur2> <kcat-default>
                                             prints a per-key table of the 6-partition landings
                                             and exits nonzero if either murmur2 column differs
                                             from the Java client's own computation
"""
import json
import sys


def keys() -> list[bytes]:
    long_x = b"x" * 1500
    return [
        b"",
        b"a", b"ab", b"abc", b"abcd",  # 1..4 bytes: tail switch 1, 2, 3 and no tail
        b"abcde", b"abcdef", b"abcdefg", b"abcdefgh",  # one tail byte longer each
        b"aaaab", b"aaaabb", b"aaaabbb", b"aaaabbbb",  # same head, differing trailing bytes
        b"aaaac", b"aaaabc", b"aaaabbc",
        b"key", b"user-1234", b"hello world", b"order/2026-10-11/7",
        "héllo".encode(), "日本語".encode(), "ключ".encode(),
        "emoji \U0001F680".encode(),
        b"\x00", b"\xff\xfe\xfd", b"\x80\x81",  # not valid UTF-8
        long_x, long_x + b"y", long_x[:-1] + b"z", b"y" * 1025,
    ]


def text_keys() -> list[str]:
    out = []
    for k in keys():
        try:
            t = k.decode("utf-8")
        except UnicodeDecodeError:
            continue
        if t and len(k) <= 64 and t.isprintable() and ":" not in t:
            out.append(t)
    return out


def landings(path: str, sep: str) -> dict[str, int]:
    """Parses `partition<sep>key` lines from a consumer's output."""
    got = {}
    for line in open(path, encoding="utf-8"):
        line = line.rstrip("\n")
        if not line:
            continue
        part, key = line.split(sep, 1)
        got[key] = int(part.removeprefix("Partition:"))
    return got


def compare(java_path: str, m2_path: str, def_path: str) -> int:
    expected = {}
    for v in json.load(open("/dev/stdin"))["vectors"]:
        if "key_hex" in v:
            try:
                expected[bytes.fromhex(v["key_hex"]).decode("utf-8")] = v["partitions"]["6"]
            except UnicodeDecodeError:
                pass
    java, m2, dflt = landings(java_path, "\t"), landings(m2_path, "\t"), landings(def_path, "\t")
    bad = 0
    print("key\texpected(java murmur2 %6)\tconsole-producer\tkcat murmur2_random\tkcat default")
    for key in text_keys():
        row = (expected[key], java.get(key), m2.get(key), dflt.get(key))
        bad += row[0] != row[1] or row[0] != row[2]
        print(key.encode("unicode_escape").decode() + "\t" + "\t".join(str(x) for x in row))
    return bad


def main() -> None:
    if sys.argv[1] == "textkeys":
        print("\n".join(text_keys()))
        return
    if sys.argv[1] == "compare":
        sys.exit(1 if compare(*sys.argv[2:5]) else 0)
    if sys.argv[1] == "keys":
        for k in keys():
            print(k.hex())
        return
    counts = [int(n) for n in sys.argv[2:]]
    out = []
    for line in sys.stdin:
        hexkey, h, parts = line.rstrip("\n").split(" ")
        raw = bytes.fromhex(hexkey)
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            text = None
        entry = {}
        if len(raw) <= 64:
            entry["key_hex"] = hexkey
        else:
            # A long key is stored as runs, [byte hex, repeat count], to keep the file small.
            runs = []
            for b in raw:
                if runs and runs[-1][0] == b:
                    runs[-1][1] += 1
                else:
                    runs.append([b, 1])
            entry["key_runs"] = [[format(b, "02x"), c] for b, c in runs]
        out.append({
            **entry,
            "key_utf8": text if len(raw) <= 64 else None,
            "key_len": len(raw),
            "murmur2": int(h),
            "partitions": {str(n): int(p) for n, p in zip(counts, parts.split(","))},
        })
    json.dump({"partition_counts": counts, "vectors": out}, sys.stdout, indent=1, ensure_ascii=False)
    print()


main()
