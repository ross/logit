# Carbon pickle differential corpus

Pickle payloads real CPython wrote, each with three readings, for carbon's restricted pickle reader
in `crates/logit-proto/src/graphite/pickle.rs`. `crates/logit-proto/tests/graphite_pickle_differential.rs`
runs the reader and the pickle-mode `GraphiteDecoder` over every case and checks them against the
readings, with no Python installed.

To regenerate, run `script/differential pickle`; to check the committed files are current, run
`script/differential pickle --check`. See [`../README.md`](../README.md).

## Provenance

| Input | Pinned at |
|---|---|
| CPython 3, which writes the `py3-*` cases and gives every reading | `python:3.12.15-slim@sha256:a6e34c598f2467ed0e9a8d349809fcd8b5c603269512df273a0bb1784edc11b1` (Python 3.12.15, `pickle.HIGHEST_PROTOCOL` 5), the version `../../interop/graphite/README.md` records for the CPython pickle captures |
| Python 2, which writes the `py2-*` cases through `cPickle`, the module Diamond and graphitesend call | `python:2.7.18-slim@sha256:6c1ffdff499e29ea663e6e67c9b6b9a3b401d554d2c9f061f9a45344e3992363` (Python 2.7.18, `pickle.HIGHEST_PROTOCOL` 2) |
| carbon's receiver, transcribed into the generator rather than installed | carbon tag `1.1.10` (commit `6fd9cd2890185195bdc69e65ca022842feaee7cc`): `lib/carbon/util.py`'s `SafeUnpickler`, `lib/carbon/protocols.py`'s `MetricPickleReceiver.stringReceived` and `MetricReceiver.metricReceived`, and `lib/carbon/conf.py`'s default `MIN_TIMESTAMP_RESOLUTION` |
| the reader's opcode lists | the "Accepted opcodes" table and the rejection paragraph after it in `crates/logit-proto/src/graphite/pickle.rs`'s module doc, read at generation time |

`script/differential pickle` prints both interpreters' versions and `pickle.HIGHEST_PROTOCOL` on
every run. The generator is `tools/differential/pickle/gen_py2.py` (Python 2 syntax, run first)
and `tools/differential/pickle/gen_cases.py` (Python 3, stdlib only, run with
`PYTHONHASHSEED=0` so a set pickles in one order). Neither container has a network.

## Files

Each case is `<case>.pkl`, the unframed payload, and `<case>.json`. A case name is
`<producer>-<subject>-p<protocol>`:

- `py3-*`: `pickle.dumps` on CPython 3.12;
- `py2-*`: `cPickle.dumps` on Python 2.7;
- `hand-*`: bytes the generator assembles in another producer's shape (og-rek's `MARK … LIST`),
  read by the same CPython.

`interop-<capture>.json` holds the readings of a recorded capture in `../../interop/graphite/`,
which stays where it is: the test reads `<capture>.raw`, checks its 4-byte big-endian length
prefix against `length_prefix` and that one frame fills the rest, and strips the prefix.

## Readings

Each JSON file is `json.dump(sort_keys=True, indent=1)`:

- `cpython`: CPython's `pickle.loads(data)`, tagged so every value survives JSON: a float as
  `{"f64": "0x<16 hex digits>", "repr": ...}`, an int as `{"int": "<decimal>"}`, a `str` as a JSON
  string or `{"surrogates": [<code points>]}` when it can't be UTF-8, bytes as `{"bytes": "<hex>"}`,
  and `{"tuple": [...]}`, `{"list": [...]}`, `{"dict": ...}`, `{"set": ...}`. A run of four or more
  equal elements is `{"repeat": n, "of": element}`. A raised exception is
  `{"error": <type>, "message": ...}`. `loads` defaults to ASCII for a Python 2 `str`, so a
  `py2-*` case with a non-ASCII `str` reads as an error here and decodes in `carbon`.
- `carbon`: the transcribed receiver. `outcome` is `ok`, `dropped` (an exception its `except`
  list catches, so the frame is ignored), or `raised` (any other, so Twisted closes the
  connection); `received` lists each `[path, timestamp, value, fate]` handed to `metricReceived`,
  where `fate` is `store`, `now` (the `-1` sentinel), `nan` (a dropped NaN value), or
  `{"raises": <type>}`; `items` is the batch's length.
- `logit`: the reader's verdict, declared by the case: `{"verdict": "ok", "datapoints": "carbon"
  | [...], "skipped": n}`, or `{"verdict": "malformed"}` with the `opcode` its error names or a
  `message` it contains.
- `divergence`: present if and only if `logit` differs from `carbon`, naming why and the
  `docs/known-gaps/mappings.md` row that records it.
- `decoder_skips`: the `logit.input.metrics.skipped` reasons the pickle decoder counts, besides
  `bad_shape`; `decoder_divergence`: present if and only if a datapoint's fate in the decoder differs
  from its carbon `fate`.
- `exercises` and `opcodes`: the opcodes the case claims, and every opcode it contains.

## What the generator checks

Generation fails, writing nothing, when:

- a case's opcodes don't include every opcode in its `exercises`;
- an `ok` case carries an opcode the reader rejects, or a `malformed` case's first rejected opcode
  isn't the one it declares;
- an `ok` case reads as `carbon`'s datapoints where carbon's frame failed;
- an accepted opcode appears in no `ok` case, or a rejected one in no `malformed` case, unless
  `gen_cases.py` lists it as unreachable with its reason: `BINUNICODE8` and `BINBYTES8`, which
  CPython writes only past 4 GiB; `LONG4`, which it writes only for an integer past the reader's
  16-byte magnitude; and `DUP`, which no pickler writes.
