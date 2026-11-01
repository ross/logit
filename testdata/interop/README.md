# Recorded interop fixtures

This directory holds real wire traffic, captured once from real third-party producers and
committed. The producers are syslog senders, collectd, OTel SDKs, carbon senders, Prometheus,
node_exporter, a `prometheus_client` app, vmagent, statsd/DogStatsD clients, nginx, Angie, freenginx, a Datadog Agent, a dd-trace tracer, four Splunk HEC
clients, Telegraf's InfluxDB v2 output, and the pod log files a kubelet leaves with containerd and
CRI-O. The fixtures check `logit`'s decoders against what those producers put on the wire:

- `crates/logit-proto/src/syslog/`
- `crates/logit-proto/src/statsd/`
- `crates/logit-proto/src/collectd/`
- `crates/logit-proto/src/otlp/`
- `crates/logit-proto/src/graphite/`
- `crates/logit-proto/src/prometheus/remote_write.rs`
- `crates/logit-proto/src/prometheus/text.rs`, through the Prometheus text differential corpus
  ([`../differential/prometheus-text/`](../differential/prometheus-text/README.md))
- `crates/logit-proto/src/datadog/`, `crates/logit-inputs/src/datadog.rs`, and
  `crates/logit-inputs/src/datadog_trace.rs`
- `crates/logit-proto/src/splunk/` and `crates/logit-inputs/src/splunk.rs`
- `crates/logit-proto/src/influxdb/`
- `crates/logit-inputs/src/cri.rs` and `crates/logit-proto/src/cri.rs`
- `crates/logit-proto/src/nginx_status.rs`

Without them, a decoder is checked only against this team's reading of RFC 3164/5424, collectd's
`network.c`, the OTLP spec, or the remote-write spec, and against `logit`'s own encoder. An encoder
and decoder that share a misunderstanding agree with each other and pass.

This file and the READMEs below it are the provenance record for committed, regeneratable test
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
  prometheus-scrape/README.md -- provenance table for prometheus-scrape/*.body
  prometheus-scrape/*.body    -- scrape response bodies node_exporter and a prometheus_client app
                                 served, byte for byte, one file per response
  prometheus-scrape/*.headers -- one sidecar per body, holding the response's Content-Type, which
                                 picks the dialect
  nginx-status/README.md  -- provenance table for nginx-status/*.body
  nginx-status/*.body     -- stub_status pages nginx, Angie, and freenginx served, byte for byte,
                             one file per response
  nginx-status/*.headers  -- one sidecar per body, holding the response's status line and headers
  statsd/README.md     -- provenance table for statsd/*.raw
  statsd/*.raw         -- raw captured UDP datagrams from two real statsd clients (Datadog's
                          `datadog` package and the plain-statsd `statsd` package), each in a
                          buffered and an unbuffered mode over one shared workload, one file per
                          datagram -- the corpus that records how a real client *packs* metrics,
                          which the hand-written grammar fixtures under
                          `crates/logit-cli/tests/fixtures/statsd/` deliberately don't
  datadog/README.md    -- provenance table for datadog/*, and what each capture settled
  datadog/*.bin        -- request bodies a real Datadog Agent sent its intake and a real dd-trace
                          tracer sent an Agent, compressed as sent, one file per request
  datadog/*.headers    -- one sidecar per body, as for prometheus/*.headers: the route, body form,
                          and compression are only recoverable from it
  datadog/*.raw        -- DogStatsD over the Agent's Unix sockets: one file per datagram, or one
                          per stream connection with its length prefixes intact
  datadog/agent-info.json -- a real Agent's `/info` document
  splunk/README.md     -- provenance table for splunk/*, and what each capture settled
  splunk/*.bin         -- HEC request bodies from the OTel Collector's `splunk_hec` exporter,
                          Docker's `splunk` log driver, SC4S, and Splunk's Java logging library,
                          compressed as sent, one file per request
  splunk/*.headers     -- one sidecar per body: the method, the route with its query string (the
                          `/raw` envelope), and the compression
  influxdb/README.md   -- provenance table for influxdb/*, and what each capture settled
  influxdb/*.bin       -- `/api/v2/write` bodies from Telegraf's `outputs.influxdb_v2`, gzip or
                          identity as sent, one file per request
  influxdb/*.headers   -- one sidecar per body: the route with its query string, and the
                          compression
  cri/README.md        -- provenance for cri/*, and what each recording settled
  cri/<runtime>/pods/  -- the kubelet's /var/log/pods tree from a containerd node (kind) and a CRI-O
                          node (minikube), rotated and gzipped files included; symlinks.txt lists
                          the /var/log/containers symlinks; not wire captures
  kafka/README.md      -- provenance for kafka/*: Java-client murmur2 partition vectors, record
                          batches as a broker stored them (Java and kcat, five codecs), and the
                          broker's ApiVersions ranges; not wire captures
```

The capture methods differ on purpose. The `*.raw` corpora come from read-only UDP and TCP sinks.
The Prometheus, Datadog, and Splunk HTTP corpora (`prometheus/*.bin`, `datadog/*.bin`,
`splunk/*.bin`) come from `raw_capture.py --proto http`, which answers `204`, because an HTTP
sender won't send another request until it gets a response; for a tracer, which reads its
Agent's answers, and a HEC client, which reads Splunk's, it replies with a configured document
instead. The OTLP corpus
(`otlp/*.json`) is the Collector's own re-emitted output. The scrape corpus
(`prometheus-scrape/*.body`) is the other direction: curl pulls each body from the exporter, as a
scraper does. Each subdirectory's README has the details.

## Regenerating

Run `script/record-fixtures [producer ...]`. That script's header comment lists every producer and
explains how to add one.

Recording is a **deliberate, reviewed act**, not part of `script/cibuild`, like `script/protogen`
and `testdata/tls/regen.sh`. It pulls real third-party images from Docker Hub and ghcr.io, and runs
them against the internet-facing package mirrors those images use: the `rsyslog`, `collectd`, and
`graphite` producers each run a fresh `apt-get install`, the `statsd`, `datadog`, and `prometheus-scrape` producers a
fresh `pip install`, and the `splunk` producer a fresh Maven build. CI shouldn't repeat that non-determinism on every push.

To regenerate:

1. Run `script/record-fixtures` by hand.
2. Review `git diff testdata/interop/`. For the binary fixtures (`collectd/*.raw`, the graphite
   pickle captures, `prometheus/*.bin`, and `datadog/*.bin`), use `git diff --stat`.
3. Commit.

A re-run doesn't reproduce these exact bytes. Container hostnames, timestamps, trace and span IDs,
every value collectd measured, and telemetrygen's synthetic OTLP attribute values all change
between runs. collectd's packing also changes which value lists land in which datagram, so file
sizes move too. That's expected: a fixture is *a* real capture from *a* real producer, not a
byte-stable golden file. See [Consuming these fixtures](#consuming-these-fixtures).

## Producer conventions

- **A producer is one `record_<name>()` function** in `script/record-fixtures`, on the shared
  `start_capture`/`finish_capture` plumbing, plus its name in `all`. Its helpers live in
  `tools/record-fixtures/`.
- **The capture sink is `tools/record-fixtures/raw_capture.py`** for every wire corpus: it writes
  whatever arrives with no parsing or re-encoding, one file per UDP datagram, per TCP connection,
  or per HTTP request. OTLP is the exception: the Collector's `file` exporter writes pdata's JSON
  marshaler output, the normative OTLP/JSON, valid whatever wire the sender used
  (`otlp/README.md`).
- **The producer is real third-party software, installed fresh at record time** when no runnable
  official image exists (`apt-get install` for rsyslog and collectd, `pip install` for the statsd
  clients). A re-record picks up the current version, so update the provenance table from the
  producer's own version output, never from memory.
- **A hand-rolled producer is acceptable only when the library under test is the thing
  hand-rolled around.** `python_graphite_pickle_producer.py` frames `pickle.dumps(...)` with the
  stdlib alone, because CPython's `pickle` is what a real carbon client runs; getting carbon's own
  Twisted client into a throwaway container would cost far more than it proves.
- **A fixed count bounds every capture**, never a duration, so a slow container start can't grow a
  fixture.
- **Each subdirectory's README carries a provenance table** (producer and version, invocation,
  date, and the construct each fixture exercises) and a "What isn't covered here (yet)" section,
  so absence is stated rather than inferred. A table, not a per-fixture sidecar, while the corpus
  stays at a handful of fixtures per producer.

## Findings worth recording

Obstacles hit while building producers, so they needn't be rediscovered:

- **Busybox's `logger` applet can't send to a remote server**: it rejects `-n` and `-P`. The
  `logger` producer uses util-linux's `logger` from `debian:bookworm-slim`.
- **SELinux blocks an unrelabeled bind mount** even when the host directory is world-writable.
  Every bind mount in `script/record-fixtures` carries `:z`, as `script/protogen`'s does.
- **`otel/opentelemetry-collector-contrib` runs as uid 10001**, so its `file` exporters fail with
  `permission denied` on a mode-755 output directory. `record_otlp` makes the directory mode 777
  first.
- **No official rsyslog or collectd image exists.** `rsyslog/rsyslog_deb12` doesn't exist, and
  the collectd project's one Docker Hub repository, `collectd/ci`, is a build environment.
  Both producers install the Debian package at record time.
- **Mounting a config over a package's own conffile stops `apt-get install` on dpkg's conffile
  prompt**, which `DEBIAN_FRONTEND=noninteractive` doesn't suppress (it reaches debconf, not dpkg).
  The rsyslog config mounts at `/etc/rsyslog-fixture.conf` and passes `-f`. Its ruleset filters on
  `$programname == 'logit-fixture'`, so rsyslogd's own startup notice stays out of the capture.
- **collectd's `network` plugin sends a datagram only when the next value list wouldn't fit under
  `MaxPacketSize` (1452), or at shutdown.** So a datagram is not a read cycle: each capture packs
  about 25 lists from several cycles, with identity elided and a cycle split across a boundary.
  Which lists land in which datagram changes on every re-record, so the tests assert identity,
  kinds, and interval, never a per-file list count. `Interval 1` fills the buffer in seconds.
- **collectd's `write_graphite` keeps one TCP connection open**, so `write-graphite-000.raw` is
  one connection's stream across three read cycles, bounded by `collectd-entrypoint.sh`'s
  `RUN_SECONDS`.
- **Pickle `protocol=-1` resolved to protocol 5** on `python:3.12-slim`; it's the capture that
  exercises `FRAME`, `SHORT_BINUNICODE`, and `MEMOIZE`.
- **The statsd corpus records packing, not grammar.** Both clients pack to their own buffer size
  (1,432 B and 512 B), not to an MTU, and neither splits a line. Its metric-type mix comes from the
  producer script's synthetic workload, not from production; `perf/load/README.md` picks its own
  type weights for that reason.
- **Telegraf streams every `/api/v2/write` body chunked**, so `raw_capture.py --accept-chunked`
  removes the chunk framing and keeps the payload verbatim.

## Not yet recorded

Producers this corpus doesn't cover yet. Each subdirectory's "What isn't covered here (yet)"
lists the constructs a covered producer still misses.

- **syslog-ng**, over any transport. A `record_syslog_ng` following `record_rsyslog`'s shape is
  the next step.
- **Docker json-file across Docker versions**, for `docker_in`.

## Size discipline

This corpus exercises decoder *paths*; it isn't a load-testing dataset. One or two representative
messages per construct is the right size. Keep fixtures to these rough sizes:

- **syslog:** a few hundred bytes per fixture.
- **collectd:** ~1.3 KB per value-list fixture, one packed datagram just under collectd's
  1452-byte `MaxPacketSize`.
- **OTLP:** low single-digit KB per fixture.
- **statsd:** ~12 KB for the whole corpus. It needs 56 small datagrams, because its subject is the
  *distribution* of datagram sizes rather than one message shape.
- **Datadog:** under 40 KB for the whole corpus. A zstd request body is small; a trace body isn't,
  since one Flask request is about ten spans, so trace captures serve few requests.
- **Splunk:** under 20 KB for the whole corpus: one request per client format or metric type.
- **Prometheus scrape:** about 150 KB at most for the whole corpus, since a scrape body is a whole
  exporter's output and can't be trimmed; shrink it by dropping a collector. As of 2026-10-09 its
  three bodies total about 31 KB.
- **CRI container logs:** about 110 KB per runtime directory. The kubelet rotates a log only past
  its `containerLogMaxSize`, and the recording needs rotated, compressed, and live files, so the
  pods write files over 24 KiB and a line over 16 KiB; trimming the payloads means a smaller
  rotation limit and a new recording.
- **Whole directory:** no fixed bound. As of 2026-10-30, the fixtures excluding READMEs total about
  410 KB: about 225 KB of CRI logs and about 180 KB for everything else. The earlier "well under
  100 KB" (92 KB as of 2026-09-25) no longer holds.

The sibling tree [`../differential/`](../differential/README.md) isn't counted here. It holds
generated cases, not captures, and its README gives its own size.

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

- `crates/logit-proto/src/syslog/decode.rs`
- `crates/logit-inputs/src/collectd.rs`: they assert a value list's data-source count and kinds,
  its `collectd.*` identity, and its interval.
- `crates/logit-inputs/src/graphite/mod.rs`: they assert a decoded path prefix, that every kind is
  a bare `Gauge`, and the pickle fixtures' exact datapoints.
- `crates/logit-proto/src/statsd/decode.rs`

None of them asserts a measured value, which differs on every run.
`crates/logit-proto/tests/prometheus_remote_write_interop.rs`,
`crates/logit-proto/tests/datadog_interop.rs`, and `crates/logit-proto/tests/splunk_interop.rs`
follow the same rule for the Prometheus, Datadog, and Splunk corpora.
`crates/logit-proto/tests/prometheus_text_differential.rs` compares each `prometheus-scrape/` body's
decode with Prometheus's own reading of the same bytes, committed under
`../differential/prometheus-text/reference/`, rather than with values written into the test.
