# Graphite/Carbon interop fixtures

Real bytes from real producers speaking both of carbon's wire protocols, captured verbatim by
`tools/record-fixtures/raw_capture.py --proto tcp`, with **no parsing and no re-encoding.** Each file
is exactly what a real sender put on the wire, and `logit`'s own `GraphiteEncoder` never touches
them. That's the point: `crates/logit-proto/src/graphite/` was written from carbon's own
`carbon/protocols.py`/`lib/carbon/protocols.py` behavior and Twisted's `Int32StringReceiver`
framing, and these fixtures check that reading against real senders.

To regenerate, run `script/record-fixtures graphite`. See `../README.md` and the header comment in
`script/record-fixtures`.

## Fixtures

Each file here is a **whole TCP connection's byte stream**, unlike `../collectd/*.raw`, which holds
one UDP datagram per file. `raw_capture.py`'s TCP mode writes one file per accepted connection, not
per message, because that's how carbon's own listeners work: a sender opens a persistent
connection and writes many lines or frames to it, rather than one connection per datapoint.

| File | Producer | Invocation | Captured | Construct exercised |
|---|---|---|---|---|
| `write-graphite-000.raw` (5760 bytes) | collectd 5.12.0-14 (Debian 12 "bookworm" `collectd-core` package, installed fresh into `debian:bookworm-slim` at record time; `../collectd/README.md` documents why there's no official runnable image) | `collectd -C /etc/collectd-fixture.conf -f` with `tools/record-fixtures/collectd-write-graphite.conf` (`Hostname "logit-fixture"`, `FQDNLookup false`, `Interval 1`, plugins `load`/`memory`/`interface`/`write_graphite`, `<Plugin write_graphite><Node "capture"> Host "capture" Port "2003" Protocol "tcp" LogSendErrors true Prefix "" StoreRates false AlwaysAppendDS false EscapeCharacter "_"`), running for `RUN_SECONDS=3` | 2026-09-13 | A real carbon plaintext **sender**: `path value timestamp\r\n` lines. Every line ends in `\r\n` (Twisted's `LineReceiver` default delimiter), so this is a real CRLF capture, not a hand-typed one (normalization 9 in `logit_proto::graphite`'s module doc). One persistent TCP connection carries 100 lines: four `Interval 1` read cycles (four distinct timestamps, 25 lines each) of `load`/`memory`/`interface` lists. Each list's data sources are flattened to one line per data source, because `write_graphite` has no multi-value wire shape either |
| `graphite-pickle-p2-000.raw` (246 bytes) | Python 3.12.14 (`python:3.12-slim`), `pickle` protocol 2, no third-party dependency | `tools/record-fixtures/python_graphite_pickle_producer.py --host capture --port 2004 --protocol 2`. That script's docstring has the fixed `DATAPOINTS` list it pickles | 2026-09-13 | One length-prefixed pickle frame, protocol 2: `PROTO`/`EMPTY_LIST`/`BINPUT`/`MARK`/`BINUNICODE`/`BININT`\|`BININT1`/`BINFLOAT`/`TUPLE2`/`APPENDS`/`STOP`. This is the plain opcode set a protocol-2 `pickle.dumps` emits, with no `FRAME`, `MEMOIZE`, or `SHORT_BINUNICODE` |
| `graphite-pickle-p5-000.raw` (230 bytes) | Same Python and image, `pickle` protocol -1 (Python's "highest available", which `pickle.HIGHEST_PROTOCOL` resolved to **5** on this image; the producer prints this on every run, so a re-record's change to this table is visible) | Same script, `--protocol -1` | 2026-09-13 | The same `DATAPOINTS` list at protocol 5. It adds three opcodes that protocol 2 never emits: `FRAME` (wraps the whole payload after `PROTO`), `SHORT_BINUNICODE` (in place of plain `BINUNICODE` for these short paths), and `MEMOIZE` (in place of `BINPUT`). All three are on the restricted reader's allowlist (`docs/adr/graphite-carbon-relay.md`'s "Pickle opcode subset", and `crates/logit-proto/src/graphite/pickle.rs`'s "Accepted opcodes") |

Both pickle fixtures pickle the **identical** `DATAPOINTS` list, a hand-picked mix of four
datapoints:

- An integer timestamp with an integer value.
- A fractional timestamp with a float value.
- A negative float value.
- A large float value.

The producer script's docstring explains why each one is there.

The collectd version is what `dpkg-query -W -f='${Version}' collectd-core` reported inside the
recording container, and the Python version is the output of `python3 --version`.
`script/record-fixtures graphite` prints both on every run: `collectd-entrypoint.sh` prints the
collectd version, and the pickle producer's docker invocation prints the Python version next to the
producer's own `pickle.HIGHEST_PROTOCOL` line. A re-record's change to this table therefore shows
up in the script's own log, the same discipline `../collectd/README.md` describes.

## Tests that consume these fixtures

`crates/logit-inputs/src/graphite/mod.rs`'s `interop_fixture_*` tests assert on the following:

- Every decoded path starts with `logit-fixture.`: the collectd config's `Hostname`, and the pickle
  producer's own hard-coded path prefix.
- Every decoded kind is a bare `Gauge`, because carbon's wire has no type.
- The pickle fixtures' exact datapoints round-trip. `interop_fixture_pickle_protocol_2_decodes`
  and `interop_fixture_pickle_protocol_5_decodes` assert the same decoded paths, timestamps, and
  values against both files, whichever opcode set produced them.
- All three fixtures decode with **no** `bad_line`, `bad_tag`, `bad_timestamp`, `non_finite_value`,
  or `bad_shape` diagnostic. This assertion covers everything not named individually.

## What isn't covered here (yet)

- **Tagged plaintext (`path;k=v value timestamp`).** collectd's `write_graphite` plugin predates
  carbon's 1.1 tag syntax and never emits it. Neither this capture nor a hand-modified config can
  produce a *real* tagged line without a different sender entirely.
  `crates/logit-proto/src/graphite/decode.rs`'s own unit tests cover tag parsing byte for byte
  instead.
- **UDP plaintext.** Carbon's plaintext wire is the same grammar over UDP or TCP
  (`logit_proto::graphite`'s module doc), and `raw_capture.py --proto udp` already exists for the
  syslog and collectd corpora. A second collectd capture wasn't judged worth it for a grammar the
  TCP fixture already exercises, byte for byte identically once framing is stripped away.
- **A real carbon/Twisted pickle *sender*.** The pickle fixtures here come from a hand-rolled
  stdlib Python script, not carbon's own `carbon-client.py` or Twisted's `pickle.dumps` call site in
  `carbon/protocols.py`. Both use the *same* CPython `pickle` module underneath, so the opcodes on
  the wire are identical either way. It's worth revisiting if carbon's own sender is ever easy to
  run in a throwaway container the way `collectd-core` is.
- **Reconnection and multiple connections.** Every fixture here is one accepted connection, so
  carbon's own reconnect behavior (a sender that drops and re-opens mid-run) isn't captured.
  `crates/logit-inputs/src/graphite/mod.rs`'s own socket tests cover multiple connections and
  connection loss against the real driver instead.
- **Protocol 0/1 pickle (text mode and the original binary protocol).** The restricted reader
  rejects protocol 0 outright (the "Rejected" list in `docs/adr/graphite-carbon-relay.md`'s
  "Pickle opcode subset", and `crates/logit-proto/src/graphite/pickle.rs`'s module doc) and
  decodes protocol 1, whose carbon payloads use only allowlisted binary opcodes. Hand-built unit
  tests with CPython-dump provenance comments in `crates/logit-proto/src/graphite/pickle.rs` cover
  both instead of a fixture (`object_construction_opcodes_are_rejected` over a protocol-0 dump,
  `a_cpython_protocol_1_dump_decodes` over a protocol-1 one), because no modern sender is left to
  capture one from: Python's own `pickle.DEFAULT_PROTOCOL` has been 3 or higher since Python 3.0.
