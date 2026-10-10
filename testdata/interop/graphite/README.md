# Graphite/Carbon interop fixtures

Real bytes from real producers speaking both of carbon's wire protocols, captured verbatim by
`tools/record-fixtures/raw_capture.py --proto tcp`, with **no parsing and no re-encoding.** Each file
is exactly what a real sender put on the wire, and `logit`'s own `GraphiteEncoder` never touches
them. That's the point: `crates/logit-proto/src/graphite/` was written from carbon's own
`carbon/protocols.py`/`lib/carbon/protocols.py` behavior and Twisted's `Int32StringReceiver`
framing, and these fixtures check that reading against real senders.

To regenerate, run `script/record-fixtures graphite`, or one of `graphite-p0`, `graphite-py2`, and
`graphite-dropwizard` to re-record that capture alone. See `../README.md` and the header comment in
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
| `graphite-pickle-p0-000.raw` (285 bytes) | Python 3.12.15 (`python:3.12-slim`), `pickle` protocol 0, no third-party dependency | Same script, `--protocol 0` | 2026-10-09 | The same `DATAPOINTS` list in protocol 0's text opcodes: `MARK`/`LIST`, then per datapoint `UNICODE` paths, `INT` and `FLOAT` lines, `TUPLE`, and `APPEND`, with a `PUT` after every string and tuple, numbered from 0 |
| `graphite-pickle-py2-000.raw` (320 bytes) | Python 2.7.18 (`python:2.7-slim`), `cPickle` | `tools/record-fixtures/python2_diamond_pickle_producer.py capture 2004`: Diamond's `GraphitePickleHandler` call, `cPickle.dumps(batch)` with no protocol argument | 2026-10-09 | Protocol 0 as Python 2 senders (Diamond, graphitesend) write it: `STRING` paths in Python 2 `repr` form, a UTF-8 path escaped as `\xc3\xa9`, a `PUT` memo numbered from **1** (`cPickle`'s numbering; Python's `pickle` and CPython 3 start at 0), a repeated path through `GET`, and a `long` timestamp as `L…L` |
| `graphite-dropwizard-000.raw` (278 bytes) | Dropwizard Metrics 4.2.25 (`metrics-graphite`), `GraphiteReporter` over `PickledGraphite`, on OpenJDK 17.0.20.1 (`maven:3.9.16-eclipse-temurin-17`) | `tools/record-fixtures/dropwizard-graphite/` (`Main.java`, `pom.xml`), built and run by `record_graphite_dropwizard` | 2026-10-09 | Protocol 0 written by hand, not by a pickler: `MARK`/`LIST`, then per datapoint `MARK STRING MARK LONG STRING TUPLE TUPLE APPEND`, no memo. The name is never escaped, so `café` arrives as raw UTF-8 inside `S'…'`, and every value is a quoted `%2.2f` string, including `'NaN'` for a NaN gauge, which decodes as a skipped `non_finite_value` |

The three CPython 3 pickle fixtures (`p0`, `p2`, `p5`) pickle the **identical** `DATAPOINTS`
list, a hand-picked mix of four datapoints:

- An integer timestamp with an integer value.
- A fractional timestamp with a float value.
- A negative float value.
- A large float value.

The producer script's docstring explains why each one is there.

The Python 2 and Dropwizard producers each pickle their own fixed list, which the docstring of
`python2_diamond_pickle_producer.py` and the doc comment of `dropwizard-graphite/Main.java` give.

The collectd version is what `dpkg-query -W -f='${Version}' collectd-core` reported inside the
recording container, and the Python version is the output of `python3 --version`.
`script/record-fixtures graphite` prints every version on every run: `collectd-entrypoint.sh`
prints the collectd version, the CPython 3 producer's docker invocation prints the Python version
next to the producer's own `pickle.HIGHEST_PROTOCOL` line, the Python 2 producer prints its
interpreter version, and the Dropwizard run prints `java -version` and the `metrics-*` jars Maven
resolved. A re-record's change to this table therefore shows up in the script's own log, the same
discipline `../collectd/README.md` describes.

## Tests that consume these fixtures

`crates/logit-inputs/src/graphite/mod.rs`'s `interop_fixture_*` tests assert on the following:

- Every decoded path starts with `logit-fixture.`: the collectd config's `Hostname`, and the pickle
  producer's own hard-coded path prefix.
- Every decoded kind is a bare `Gauge`, because carbon's wire has no type.
- The pickle fixtures' exact datapoints round-trip. `interop_fixture_pickle_protocol_0_decodes`,
  `interop_fixture_pickle_protocol_2_decodes`, and `interop_fixture_pickle_protocol_5_decodes`
  assert the same decoded paths, timestamps, and values against all three CPython 3 files,
  whichever opcode set produced them. `interop_fixture_pickle_python_2_decodes` and
  `interop_fixture_dropwizard_decodes` assert their own producers' lists, non-ASCII paths
  included.
- Every fixture decodes with **no** `bad_line`, `bad_tag`, `bad_timestamp`, `non_finite_value`,
  or `bad_shape` diagnostic, except the Dropwizard capture's one `non_finite_value` for its NaN
  gauge. This assertion covers everything not named individually.

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
- **A real carbon/Twisted pickle *sender*.** The CPython 3 pickle fixtures here come from a
  hand-rolled stdlib Python script, not carbon's own `carbon-client.py` or Twisted's `pickle.dumps` call site in
  `carbon/protocols.py`. Both use the *same* CPython `pickle` module underneath, so the opcodes on
  the wire are identical either way. It's worth revisiting if carbon's own sender is ever easy to
  run in a throwaway container the way `collectd-core` is.
- **Reconnection and multiple connections.** Every fixture here is one accepted connection, so
  carbon's own reconnect behavior (a sender that drops and re-opens mid-run) isn't captured.
  `crates/logit-inputs/src/graphite/mod.rs`'s own socket tests cover multiple connections and
  connection loss against the real driver instead.
- **Protocol 1 pickle (the original binary protocol).** No surveyed sender writes it, and every
  opcode a protocol-1 carbon payload uses is a binary one the reader also accepts at protocol 2.
  `a_cpython_protocol_1_dump_decodes` in `crates/logit-proto/src/graphite/pickle.rs` covers it
  with a committed CPython dump instead of a fixture.
