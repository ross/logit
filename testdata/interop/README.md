# Recorded interop fixtures

Real wire traffic captured once from real third-party producers (syslog senders, OTel SDKs) and
committed here, so `logit`'s decoders (`crates/logit-inputs/src/syslog.rs`,
`crates/logit-proto/src/otlp/`) get checked against what those producers actually put on the
wire -- not only against this team's own reading of RFC 3164/5424 or the OTLP spec, and not only
against `logit`'s own encoder, which will happily agree with itself even if both sides share the
same misunderstanding. See [`docs/plans/recorded-interop-fixtures.md`](../../docs/plans/recorded-interop-fixtures.md)
for the full design and rationale; this file and the two below it are the provenance record
[ADR `committed-pregenerated-otlp-protobuf`](../../docs/adr/committed-pregenerated-otlp-protobuf.md)
and `testdata/tls/README.md` both establish for committed, regeneratable test artifacts.

**Not used at runtime** -- nothing under `crates/` reads this directory outside `#[cfg(test)]`
code, same as `testdata/tls/`.

## Layout

```
testdata/interop/
  syslog/README.md   -- provenance table for syslog/*.raw
  syslog/*.raw        -- raw captured UDP datagrams, exactly as received, one file per message
  otlp/README.md      -- provenance table for otlp/*.json
  otlp/*.json          -- OTLP/JSON as re-emitted by the Collector's own `file` exporter
```

`syslog/*.raw` and `otlp/*.json` are captured differently on purpose, not inconsistently -- see
each subdirectory's own README for why.

## Regenerating

`script/record-fixtures [producer ...]` -- see that script's own header comment for the full
producer list and how to add one. Like `script/protogen` and `testdata/tls/regen.sh`, this is a
**deliberate, reviewed act**, not part of `script/cibuild`: it pulls real third-party images from
Docker Hub/ghcr.io and runs them against the internet-facing package mirrors those images
themselves use (`rsyslog`'s producer in particular does a fresh `apt-get install` every run), which
is exactly the kind of non-determinism CI should never re-run on every push. Run it by hand, read
`git diff testdata/interop/` (a plain-text/JSON diff, since nothing here is binary), and commit.

Re-running won't reproduce these exact bytes -- container hostnames, timestamps, trace/span ids,
and (for OTLP) telemetrygen's synthetic attribute values all change between runs. That's expected
and fine: the point of a fixture is that it's *a* real capture from *a* real producer, not a
byte-stable golden file: see "Consuming these fixtures" below.

## Size discipline

Every fixture here is a handful of syslog datagrams or a few KB of OTLP/JSON -- there's no reason
for one to be bigger. As a rule of thumb: **a few hundred bytes per syslog fixture, low
single-digit KB per OTLP fixture, and this whole directory should stay well under 100 KB total**
(it's a few KB as of this writing). If a producer's natural output is bigger than that (a verbose
OTLP payload with many spans, say), trim it at record time -- `script/record-fixtures`'s OTLP
producer already does this (`--traces=3`/`--logs=3`/`--metrics=3`, not an open-ended `--duration`)
rather than committing everything a producer happens to emit. This corpus exists to exercise
decoder *paths*, not to be a load-testing dataset -- one or two representative messages per
construct is the right size, and a growing per-fixture size is a sign something needless is
creeping in (padding, verbose repeated attributes, an accidentally-large `--count`).

## Consuming these fixtures

Tests that read these files should assert on **identifiable decoded values** (the message content,
a hostname, a trace id, a span name) rather than on the raw bytes changing or not changing --
see `crates/logit-inputs/src/syslog.rs`'s `interop_fixture_*` tests for the pattern. The fixtures
themselves are allowed to change shape on a re-record (different container hostname, different
timestamp); a test asserting byte-for-byte fixture equality would be testing this directory's own
stability, not `logit`'s decoder.
