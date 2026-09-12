# Recorded interop fixtures

Real wire traffic captured once from real third-party producers (syslog senders, collectd, OTel
SDKs) and committed here, so `logit`'s decoders (`crates/logit-inputs/src/syslog.rs`,
`crates/logit-proto/src/collectd/`, `crates/logit-proto/src/otlp/`) get checked against what those
producers actually put on the wire -- not only against this team's own reading of RFC 3164/5424,
collectd's `network.c` or the OTLP spec, and not only against `logit`'s own encoder, which will
happily agree with itself even if both sides share the same misunderstanding. See
[`docs/plans/recorded-interop-fixtures.md`](../../docs/plans/recorded-interop-fixtures.md)
for the full design and rationale; this file and the three below it are the provenance record
[ADR `committed-pregenerated-otlp-protobuf`](../../docs/adr/committed-pregenerated-otlp-protobuf.md)
and `testdata/tls/README.md` both establish for committed, regeneratable test artifacts.

**Not used at runtime** -- nothing under `crates/` reads this directory outside `#[cfg(test)]`
code, same as `testdata/tls/`.

## Layout

```
testdata/interop/
  syslog/README.md    -- provenance table for syslog/*.raw
  syslog/*.raw        -- raw captured UDP datagrams, exactly as received, one file per message
  collectd/README.md  -- provenance table for collectd/*.raw
  collectd/*.raw      -- raw captured UDP datagrams from collectd's own binary `network` plugin,
                         one file per datagram (each one packs many value lists)
  otlp/README.md      -- provenance table for otlp/*.json
  otlp/*.json         -- OTLP/JSON as re-emitted by the Collector's own `file` exporter
```

`syslog/*.raw`/`collectd/*.raw` and `otlp/*.json` are captured differently on purpose, not
inconsistently -- see each subdirectory's own README for why.

## Regenerating

`script/record-fixtures [producer ...]` -- see that script's own header comment for the full
producer list and how to add one. Like `script/protogen` and `testdata/tls/regen.sh`, this is a
**deliberate, reviewed act**, not part of `script/cibuild`: it pulls real third-party images from
Docker Hub/ghcr.io and runs them against the internet-facing package mirrors those images
themselves use (the `rsyslog` and `collectd` producers in particular each do a fresh `apt-get
install` every run), which is exactly the kind of non-determinism CI should never re-run on every
push. Run it by hand, read `git diff testdata/interop/` (`git diff --stat` for `collectd/*.raw`,
which is the one genuinely binary corner of this directory), and commit.

Re-running won't reproduce these exact bytes -- container hostnames, timestamps, trace/span ids,
every value collectd actually measured, and (for OTLP) telemetrygen's synthetic attribute values
all change between runs; collectd's own packing even changes how many value lists land in which
datagram, so the *file sizes* move too. That's expected and fine: the point of a fixture is that
it's *a* real capture from *a* real producer, not a byte-stable golden file: see "Consuming these
fixtures" below.

## Size discipline

Every fixture here is a handful of syslog datagrams, one collectd datagram, or a few KB of
OTLP/JSON -- there's no reason for one to be bigger. As a rule of thumb: **a few hundred bytes per
syslog fixture, one datagram (so ~1.3 KB, collectd's own `MaxPacketSize`) per collectd fixture, low
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
see `crates/logit-inputs/src/syslog.rs`'s and `crates/logit-inputs/src/collectd.rs`'s
`interop_fixture_*` tests for the pattern (the collectd ones assert a list's data-source count and
kinds, its `collectd.*` identity and its interval -- never a measured value, which is different
every run). The fixtures
themselves are allowed to change shape on a re-record (different container hostname, different
timestamp); a test asserting byte-for-byte fixture equality would be testing this directory's own
stability, not `logit`'s decoder.
