# Graphite/Carbon interop fixtures

Real bytes from real producers speaking both of carbon's wire protocols, captured verbatim by
`tools/record-fixtures/raw_capture.py --proto tcp` -- **no parsing, no re-encoding.** Each file is
exactly what a real sender put on the wire; `logit`'s own `GraphiteEncoder` never touches these,
which is the whole point: `crates/logit-proto/src/graphite/` was written from carbon's own
`carbon/protocols.py`/`lib/carbon/protocols.py` behavior and Twisted's `Int32StringReceiver`
framing, and these fixtures are what checks that reading against real senders. Regenerate with
`script/record-fixtures graphite` (see `../README.md` and `script/record-fixtures`'s own header
comment).

Unlike `../collectd/*.raw` (one UDP datagram per file), every file here is a **whole TCP
connection's byte stream** -- `raw_capture.py`'s TCP mode writes one file per accepted connection,
not per message, because carbon's own listeners are exactly that: a persistent connection a sender
writes many lines or frames to, not one connection per datapoint.

| File | Producer | Invocation | Captured | Construct exercised |
|---|---|---|---|---|
| `write-graphite-000.raw` (5760 bytes) | collectd 5.12.0-14 (Debian 12 "bookworm" `collectd-core` package, installed fresh into `debian:bookworm-slim` at record time -- same no-official-runnable-image situation `../collectd/README.md` documents) | `collectd -C /etc/collectd-fixture.conf -f` with `tools/record-fixtures/collectd-write-graphite.conf` (`Hostname "logit-fixture"`, `Interval 1`, plugins `load`/`memory`/`interface`/`write_graphite`, `<Plugin write_graphite><Node "capture"> Host "capture" Port "2003" Protocol "tcp" Prefix "" StoreRates false AlwaysAppendDS false EscapeCharacter "_"`), running for `RUN_SECONDS=3` | 2026-09-13 | A real carbon plaintext **sender**: `path value timestamp\r\n` lines (Twisted's `LineReceiver` default delimiter is `\r\n`, so this is a real CRLF capture, not a hand-typed one -- normalization 9 in `logit_proto::graphite`'s module doc). One persistent TCP connection carrying three `Interval 1` read cycles' worth of `load`/`memory`/`interface` lists, each list's data sources flattened to one line per data source (`write_graphite` has no multi-value wire shape either) |
| `graphite-pickle-p2-000.raw` (246 bytes) | Python 3.12.14 (`python:3.12-slim`), `pickle` protocol 2, no third-party dependency | `tools/record-fixtures/python_graphite_pickle_producer.py --host capture --port 2004 --protocol 2` -- see that script's own docstring for the fixed `DATAPOINTS` list it pickles | 2026-09-13 | One length-prefixed pickle frame, protocol 2: `PROTO`/`EMPTY_LIST`/`BINPUT`/`MARK`/`BINUNICODE`/`BININT`\|`LONG1`/`BINFLOAT`/`TUPLE2`/`APPENDS`/`STOP` -- the plain opcode set a protocol-2 `pickle.dumps` emits, with no `FRAME`/`MEMOIZE`/`SHORT_BINUNICODE` |
| `graphite-pickle-p5-000.raw` (230 bytes) | Same Python/image, `pickle` protocol -1 (Python's "highest available", which `pickle.HIGHEST_PROTOCOL` resolved to **5** on this image -- the producer prints this every run so a re-record's diff to this table is visible) | Same script, `--protocol -1` | 2026-09-13 | The same `DATAPOINTS` list, protocol 5: adds `FRAME` (wraps the whole payload after `PROTO`), `SHORT_BINUNICODE` (in place of plain `BINUNICODE` for these short paths) and `MEMOIZE` (in place of `BINPUT`) -- three opcodes protocol 2 never emits, all three on the restricted reader's allow-list (`crates/logit-proto/src/graphite/mod.rs`'s "Pickle opcode subset") |

Both pickle fixtures pickle the **identical** `DATAPOINTS` list (a hand-picked mix: an integer
timestamp with an integer value, a fractional timestamp with a float value, a negative float
value, and a large float value -- see the producer script's own docstring for why each one is
there), so `crates/logit-inputs/src/graphite/mod.rs`'s `interop_fixture_pickle_protocol_2_decodes`
and `interop_fixture_pickle_protocol_5_decodes` assert the exact same decoded paths/timestamps/
values against both files, whichever opcode set produced them.

The collectd version is what `dpkg-query -W -f='${Version}' collectd-core` reported inside the
recording container; the Python version is `python3 --version`'s own output. Both are printed by
`script/record-fixtures graphite`'s own output every run (`collectd-entrypoint.sh` for the
collectd version, the pickle producer's docker invocation and its own
`pickle.HIGHEST_PROTOCOL` line for the Python one), so a re-record's diff to this table is visible
in the script's own log, the same discipline `../collectd/README.md` describes.

`crates/logit-inputs/src/graphite/mod.rs`'s `interop_fixture_*` tests consume these: that every
decoded path starts with `logit-fixture.` (the collectd config's `Hostname`, and the pickle
producer's own hard-coded path prefix), that every decoded kind is a bare `Gauge` (carbon's wire
has no type), that the pickle fixtures' exact datapoints round-trip, and -- the assertion that
covers everything not named individually -- that all three fixtures decode with **no**
`bad_line`, `bad_tag`, `bad_timestamp`, `non_finite_value` or `bad_shape` diagnostic.

## What isn't covered here (yet)

- **Tagged plaintext (`path;k=v value timestamp`).** collectd's `write_graphite` plugin predates
  carbon's 1.1 tag syntax and never emits it; neither this capture nor a hand-modified config can
  produce a *real* tagged line without a different sender entirely. `crates/logit-proto/src/
  graphite/decode.rs`'s own unit tests cover tag parsing byte-for-byte instead.
- **UDP plaintext.** Carbon's plaintext wire is the same grammar over UDP or TCP
  (`logit_proto::graphite`'s module doc), and `raw_capture.py --proto udp` already exists for the
  syslog/collectd corpora -- this just wasn't judged worth a second collectd capture for a grammar
  the TCP fixture already exercises byte-for-byte identically once framing is stripped away.
- **A real carbon/Twisted pickle *sender*.** The pickle fixtures here come from a hand-rolled
  stdlib Python script, not carbon's own `carbon-client.py` or Twisted's `pickle.dumps` call site
  in `carbon/protocols.py` -- but both are the *same* CPython `pickle` module underneath, so the
  opcodes on the wire are identical either way. Worth revisiting if carbon's own sender is ever
  easy to run in a throwaway container the way `collectd-core` is.
- **Reconnection / multiple connections.** Every fixture here is one accepted connection; carbon's
  own reconnect behavior (a sender that drops and re-opens mid-run) isn't captured. `crates/
  logit-inputs/src/graphite/mod.rs`'s own socket tests cover multiple connections and connection
  loss against the real driver instead.
- **Protocol 0/1 pickle (text-mode / the original binary protocol).** Both are rejected outright
  by the restricted reader (`crates/logit-proto/src/graphite/mod.rs`'s "Rejected" list) and are
  covered by hand-built unit tests with CPython-dump provenance comments in `crates/logit-proto/
  src/graphite/pickle.rs`, not by a fixture -- there is no real modern sender left to capture one
  from (Python's own `pickle.DEFAULT_PROTOCOL` has been ≥ 2 since Python 2.3).
