"""The `sample` transform's keyed verdict, redone in Python, so a scenario can say which keys
a `sample` node keeps before a run and a reader can check it with no `logit` built.

`logit_core::sampling` (crates/logit-core/src/sampling.rs) is the contract: XXH64 with seed 0
over a key's canonical bytes (a string's own bytes; an integer's decimal text), kept when the
hash's top 53 bits fall below `rate × 2^53`. At `rate: 0.5` that's a hash below 2^63, a clear
top bit. `xxh64` is xxHash's `doc/xxhash_spec.md` for XXH64, and `VECTORS` holds the published
seed-0 vectors and the contract's own pinned ones, which selftest.py checks it against.
"""

_MASK = (1 << 64) - 1
_P1 = 0x9E3779B185EBCA87
_P2 = 0xC2B2AE3D27D4EB4F
_P3 = 0x165667B19E3779F9
_P4 = 0x85EBCA77C2B2AE63
_P5 = 0x27D4EB2F165667C5

# `(bytes, XXH64 seed 0)`: the published vectors, then the canonical bytes of each keyable value
# `logit_core::sampling`'s tests pin (`I64(200)` is `b"200"`, `F64(-0.0)` is `b"-0"`, a
# timestamp is its decimal nanoseconds, a trace id its 32 lowercase hex characters).
VECTORS = (
    (b"", 0xEF46DB3751D8E999),
    (b"a", 0xD24EC4F1A98C6E5B),
    (b"abc", 0x44BC2CF5AD770999),
    (b"200", 0x25436E545A8BCC7C),
    (b"-7", 0x0C4EC0C6AD41198F),
    (b"18446744073709551615", 0x8D1F8F6D8DFA7C47),
    (b"0.5", 0x1DC89AF31B8D1B23),
    (b"-0", 0x8E56E730C4927269),
    (b"true", 0xD7C9B97948142E4A),
    (b"false", 0x6D3F99CCC0C03A7A),
    (b"1700000000000000000", 0x4F042F179CD33215),
    (b"4bf92f3577b34da6a3ce929d0e0e4736", 0x2AB28D25664B8CFD),
)


def _rotl(x, r):
    return ((x << r) | (x >> (64 - r))) & _MASK


def _round(acc, lane):
    acc = (acc + lane * _P2) & _MASK
    return (_rotl(acc, 31) * _P1) & _MASK


def _lane(data, at, width):
    return int.from_bytes(data[at:at + width], "little")


def xxh64(data, seed=0):
    """XXH64 of `data` under `seed`, as xxHash's spec writes it."""
    n, i = len(data), 0
    if n >= 32:
        acc = [(seed + _P1 + _P2) & _MASK, (seed + _P2) & _MASK, seed & _MASK,
               (seed - _P1) & _MASK]
        while i + 32 <= n:
            acc = [_round(acc[k], _lane(data, i + 8 * k, 8)) for k in range(4)]
            i += 32
        h = (_rotl(acc[0], 1) + _rotl(acc[1], 7) + _rotl(acc[2], 12) + _rotl(acc[3], 18)) & _MASK
        for one in acc:
            h = ((h ^ _round(0, one)) * _P1 + _P4) & _MASK
    else:
        h = (seed + _P5) & _MASK
    h = (h + n) & _MASK
    while i + 8 <= n:
        h = (_rotl(h ^ _round(0, _lane(data, i, 8)), 27) * _P1 + _P4) & _MASK
        i += 8
    if i + 4 <= n:
        h = (_rotl(h ^ ((_lane(data, i, 4) * _P1) & _MASK), 23) * _P2 + _P3) & _MASK
        i += 4
    while i < n:
        h = (_rotl(h ^ ((data[i] * _P5) & _MASK), 11) * _P1) & _MASK
        i += 1
    h ^= h >> 33
    h = (h * _P2) & _MASK
    h ^= h >> 29
    h = (h * _P3) & _MASK
    return h ^ (h >> 32)


def keep(x, rate):
    """`logit_core::sampling::keep`: whether a 64-bit hash `x` falls inside `rate`."""
    if not rate < 1.0:
        return True
    if rate <= 0.0:
        return False
    return (x >> 11) < int(rate * (1 << 53))


def verdicts(keys, rate):
    """`{key: (hash, kept)}` for each string key, as a `sample` node keyed on an attribute
    that holds it decides."""
    out = {}
    for key in keys:
        x = xxh64(str(key).encode())
        out[str(key)] = (x, keep(x, rate))
    return out
