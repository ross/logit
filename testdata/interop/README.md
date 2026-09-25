# Recorded interop fixtures

This directory holds real wire traffic, captured once from real third-party producers and
committed. The producers are syslog senders, collectd, OTel SDKs, carbon senders, Prometheus,
vmagent, and statsd/DogStatsD clients. The fixtures check `logit`'s decoders against what those producers put on
the wire:

- `crates/logit-inputs/src/syslog.rs`
- `crates/logit-inputs/src/statsd.rs`
- `crates/logit-proto/src/collectd/`
- `crates/logit-proto/src/otlp/`
- `crates/logit-proto/src/graphite/`
- `crates/logit-proto/src/prometheus/remote_write.rs`

Without them, a decoder is checked only against this team's reading of RFC 3164/5424, collectd's
`network.c`, the OTLP spec, or the remote-write spec, and against `logit`'s own encoder. An encoder
and decoder that share a misunderstanding agree with each other and pass.

For the design and rationale, see
[`docs/plans/recorded-interop-fixtures.md`](../../docs/plans/recorded-interop-fixtures.md). This
file and the READMEs below it are the provenance record for committed, regeneratable test
artifacts that [ADR `committed-pregenerated-otlp-protobuf`](../../docs/adr/committed-pregenerated-otlp-protobuf.md)
and `testdata/tls/README.md` both establish.

**Not used at runtime.** Nothing under `crates/` reads this directory outside `#[cfg(test)]` code,
the same as `testdata/tls/`.

## Layout

Each subdirectory has a README with a provenance table for its fixtures.

```
testdata/interop/
  syslog/README.md     -- provenance table for syslog/*.raw
  syslog/*.raw         -- raw captured syslog messages, exactly as received, one file per message:
                          UDP datagrams, plus one TCP connection stream (rsyslog-tcp-000.raw)
  collectd/README.md   -- provenance table for collectd/*.raw
  collectd/*.raw       -- raw captured UDP datagrams from collectd's own binary `network` plugin,
                          one file per datagram (each collectd-00N.raw packs many value lists;
                          collectd-notification-000.raw carries a `threshold` notification)
  otlp/README.md       -- provenance table for otlp/*.json
  otlp/*.json          -- OTLP/JSON as re-emitted by the Collector's own `file` exporter
  graphite/README.md   -- provenance table for graphite/*.raw
  graphite/*.raw       -- raw captured TCP connection streams: real carbon plaintext (collectd's
                          `write_graphite` plugin) and real carbon pickle frames (a stdlib Python
                          producer), one file per accepted connection
  prometheus/README.md -- provenance table for prometheus/*.bin
  prometheus/*.bin     -- compressed protobuf remote-write request bodies, exactly as a real
                          Prometheus (Snappy) or a real vmagent (zstd, its default wire, and Snappy)
                          POSTed them, one file per request
  prometheus/*.headers -- one sidecar per body, holding that request's method, path and request
                          headers -- which is what carries the `Content-Type` and
                          `X-Prometheus-Remote-Write-Version` the wire version is read from, and
                          the `Content-Encoding` that says which decompressor the body needs
  statsd/README.md     -- provenance table for statsd/*.raw
  statsd/*.raw         -- raw captured UDP datagrams from two real statsd clients (Datadog's
                          `datadog` package and the plain-statsd `statsd` package), each in a
                          buffered and an unbuffered mode over one shared workload, one file per
                          datagram -- the corpus that records how a real client *packs* metrics,
                          which the hand-written grammar fixtures under
                          `crates/logit-cli/tests/fixtures/statsd/` deliberately don't
```

The capture methods differ on purpose. The `*.raw` corpora come from read-only UDP and TCP sinks.
The Prometheus corpus (`prometheus/*.bin`) comes from `raw_capture.py --proto http`, which answers
`204`, because an HTTP sender won't send another request until it gets a response. The OTLP corpus
(`otlp/*.json`) is the Collector's own re-emitted output. Each subdirectory's README has the
details.

## Regenerating

Run `script/record-fixtures [producer ...]`. That script's header comment lists every producer and
explains how to add one.

Recording is a **deliberate, reviewed act**, not part of `script/cibuild`, like `script/protogen`
and `testdata/tls/regen.sh`. It pulls real third-party images from Docker Hub and ghcr.io, and runs
them against the internet-facing package mirrors those images use: the `rsyslog`, `collectd`, and
`graphite` producers each run a fresh `apt-get install`, and the `statsd` producer a fresh
`pip install`. CI shouldn't repeat that non-determinism on every push.

To regenerate:

1. Run `script/record-fixtures` by hand.
2. Review `git diff testdata/interop/`. For the binary fixtures (`collectd/*.raw`, the graphite
   pickle captures, and `prometheus/*.bin`), use `git diff --stat`.
3. Commit.

A re-run doesn't reproduce these exact bytes. Container hostnames, timestamps, trace and span IDs,
every value collectd measured, and telemetrygen's synthetic OTLP attribute values all change
between runs. collectd's packing also changes which value lists land in which datagram, so file
sizes move too. That's expected: a fixture is *a* real capture from *a* real producer, not a
byte-stable golden file. See [Consuming these fixtures](#consuming-these-fixtures).

## Size discipline

This corpus exercises decoder *paths*; it isn't a load-testing dataset. One or two representative
messages per construct is the right size. Keep fixtures to these rough sizes:

- **syslog:** a few hundred bytes per fixture.
- **collectd:** ~1.3 KB per value-list fixture, one packed datagram just under collectd's
  1452-byte `MaxPacketSize`.
- **OTLP:** low single-digit KB per fixture.
- **statsd:** ~12 KB for the whole corpus. It needs 56 small datagrams, because its subject is the
  *distribution* of datagram sizes rather than one message shape.
- **Whole directory:** well under 100 KB total. As of 2026-09-24, the fixtures, excluding READMEs,
  total about 37 KB.

If a producer's natural output is bigger, such as a verbose OTLP payload with many spans, trim it
at record time instead of committing everything the producer emits. `script/record-fixtures`'s
OTLP producer does this with `--traces=3`/`--logs=3`/`--metrics=3` instead of an open-ended
`--duration`. A growing per-fixture size signals something needless creeping in, such as padding,
verbose repeated attributes, or an accidentally large `--count`.

## Consuming these fixtures

Tests that read these files must assert on **identifiable decoded values**, such as the message
content, a hostname, a trace ID, or a span name, not on whether the raw bytes changed. The
fixtures can change shape on a re-record (a different container hostname or timestamp). A test
asserting byte-for-byte fixture equality tests this directory's stability, not `logit`'s decoder.

For the pattern, see the `interop_fixture_*` tests in these files:

- `crates/logit-inputs/src/syslog.rs`
- `crates/logit-inputs/src/collectd.rs`: they assert a value list's data-source count and kinds,
  its `collectd.*` identity, and its interval.
- `crates/logit-inputs/src/graphite/mod.rs`: they assert a decoded path prefix, that every kind is
  a bare `Gauge`, and the pickle fixtures' exact datapoints.
- `crates/logit-inputs/src/statsd.rs`

None of them asserts a measured value, which differs on every run.
`crates/logit-proto/tests/prometheus_remote_write_interop.rs` follows the same rule for the
Prometheus corpus.
