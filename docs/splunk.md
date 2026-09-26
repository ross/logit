# Splunk and `logit`

`logit` speaks Splunk's HTTP Event Collector (HEC) in both directions. It sends logs, metrics,
and spans to Splunk Enterprise and Splunk Cloud Platform over HEC, in the shape the
OpenTelemetry Collector's `splunk_hec` exporter writes, so an index fed by both tools holds one
schema. It also stands where Splunk's HEC port stands, taking what any HEC client sends (Docker's
`splunk` log driver, Splunk's logging libraries, the OTel exporter, SC4S) losslessly. A client
needs only a new URL to point at `logit` instead of Splunk, which makes a migration to or from
Splunk a configuration change rather than an application change.

This doc helps you pick a topology and avoid the mistakes that lose data. Each component's
configuration and telemetry are in [`docs/deploying.md`](deploying.md):
[`splunk_hec_in`](deploying.md#splunk_hec_in-standing-in-for-splunks-hec) and
[`splunk_hec_out`](deploying.md#splunk_hec_out-sending-to-splunk-over-hec).

Related docs:

- [ADR `splunk-hec-relay`](adr/splunk-hec-relay.md): the design decisions, including why
  `logit` uses the OTel exporter's attribute names rather than a `splunk.*` namespace.
- [`docs/plans/splunk-relay.md`](plans/splunk-relay.md): the protocol survey, what Splunk accepts
  and emits, and how each Splunk concept maps onto `logit`'s event model.
- `crates/logit-proto/src/splunk/mod.rs`'s module doc: the canonical mapping tables and the
  permitted normalizations of the `splunk_hec_in -> splunk_hec_out` relay.
- [`docs/known-gaps.md`](known-gaps.md)'s "Splunk" section: what isn't built or isn't verified.

## Topologies

Splunk is two products with separate stores. Splunk Enterprise and Splunk Cloud Platform (the
same product, hosted) keep logs in event indexes and metrics in metrics indexes, and have no trace
store. Splunk Observability Cloud keeps traces and metrics, and no logs. Four topologies cover the
ways `logit` sits next to them:

- **Direct to the Platform over HEC.** `logit` is the host collector and posts to Splunk's HEC
  with `splunk_hec_out`: [`fixtures/splunk-hec-send.yaml`](../fixtures/splunk-hec-send.yaml)
  tails a log file, stamps the index, source, sourcetype, and host, and sends it.
- **Through SC4S or the Splunk OTel Collector.** `logit` hands data to Splunk's own collectors,
  which send HEC on: `syslog_out` to Splunk Connect for Syslog (SC4S), or `otlp_out` to the Splunk
  OTel Collector's OTLP receiver. No Splunk-specific component is involved.
- **Standing in for HEC.** HEC clients point at `splunk_hec_in` instead of Splunk:
  [`fixtures/splunk-hec-receive.yaml`](../fixtures/splunk-hec-receive.yaml) has the listener and
  the client-side settings for Docker's driver, the OTel exporter, and Splunk's Java appender.
  [`fixtures/splunk-hec-relay.yaml`](../fixtures/splunk-hec-relay.yaml) relays what arrives on to
  Splunk, the tee a migration runs through
  ([From Splunk](#from-splunk-tee-compare-then-cut-over)).
- **Observability Cloud over OTLP.** `otlp_out` posts traces and metrics to Observability Cloud's
  OTLP ingest with an `X-SF-Token` header:
  [`fixtures/splunk-observability.yaml`](../fixtures/splunk-observability.yaml). This leg is
  unverified ([What's verified](#whats-verified)).

Which component carries each signal in each topology:

| Signal | Direct over HEC | Through SC4S or the Collector | HEC stand-in | Observability Cloud |
|---|---|---|---|---|
| Logs | `splunk_hec_out` | `syslog_out` to SC4S; `otlp_out` to the Collector | `splunk_hec_in` (`/event` and `/raw`) | none: no log store |
| Metrics | `splunk_hec_out`: gauges and sums natively, other kinds under `multi_value: expand` | `otlp_out` to the Collector | `splunk_hec_in` | `otlp_out` over OTLP/HTTP |
| Traces | `splunk_hec_out`, spans as searchable JSON events (the Platform has no trace view) | `otlp_out` to the Collector | `splunk_hec_in`, which decodes the exporter's span objects back to spans | `otlp_out` over OTLP/HTTP or OTLP/gRPC |

## Which way to send

### To Splunk: direct or through Splunk's collectors

**Send logs and metrics directly with `splunk_hec_out` when `logit` is already the collector on
the host.** It's one hop, it works wherever HTTPS does, it's the same wire on Enterprise and
Cloud, and every attribute arrives as an indexed field with no props or transforms stage. **Send
traces and Observability Cloud metrics with `otlp_out`.** Whichever you pick, **don't send one
signal both ways**: Splunk indexes both copies.

Sending directly costs you:

- acknowledgment on a Splunk Cloud stack that doesn't offer it, where delivery ends at a `200`
  ([below](#acknowledgment-is-off-by-default-and-your-splunk-cloud-stack-may-not-offer-it));
- per-token index allowlists to keep in step with the `index` you stamp;
- one number per metric name, so histograms, summaries, and sketches need
  [`multi_value: expand`](#multi-number-metrics-are-dropped-unless-you-set-multi_value-expand).

Front SC4S only where you want its vendor syslog parsers: it's one more container, it takes
only syslog, and it parses again what `logit` already parsed. The Splunk OTel Collector is
Splunk's supported path for OTel data and covers the Observability Cloud legs in one agent, but
`otlp_out` to the Collector is two hops to reach the HEC endpoint `splunk_hec_out` reaches in
one.

### From Splunk: tee, compare, then cut over

A migration away from Splunk runs through the HEC stand-in in three stages:

1. **Tee.** Point one HEC client at `splunk_hec_in` (a Docker daemon's `splunk-url`, a Java
   appender's `url`, an OTel exporter's `endpoint`) and fan out to `splunk_hec_out` beside the new
   backend, as `splunk-hec-relay.yaml` does. Splunk keeps receiving that client's data, no
   application changes, and undoing it is one URL edit.
2. **Compare.** Check the new backend against Splunk, then move the rest of the clients.
3. **Cut over.** Remove the `splunk_hec_out` leg.

A universal forwarder can't be teed this way, because it speaks only Splunk-to-Splunk (S2S), which
`logit` doesn't ([No S2S](#no-s2s-a-universal-forwarder-cant-point-at-logit)).

## Rules that lose data when missed

### Multi-number metrics are dropped unless you set `multi_value: expand`

A Splunk metric is one number per name. Under `multi_value: skip`, the default, `splunk_hec_out`
drops a `Histogram`, `Summary`, `Distribution`, `Set`, `Samples`, or `SetMembers` record, counted
`logit.output.metrics.skipped{metric_kind}`. `multi_value: expand` writes each as a series set
instead, counted `logit.output.metrics.degraded{metric_kind}`: a histogram or summary in the OTel
exporter's shape (`_sum`, `_count`, cumulative `_bucket` with an `le` dimension, or `<name>_<q>`
with `qt`), so an `mstats` dashboard using `histperc` reads it unchanged, and the rest as count,
sum, and percentile series. An `ExponentialHistogram` is dropped under both, as the exporter
does. The codec's module doc has the exact series.

A `Sum` goes out as its value with `metric_type` `Sum`. HEC has no carrier for temporality, so a
delta `Sum` reads in Splunk as a series of samples, not a running total: put `aggregate` with
`temporality: cumulative` in front if a dashboard expects a counter.

### `index`, `source`, `sourcetype`, and `host` come from the resource

`splunk_hec_out` has no per-sink `index` or `sourcetype` field. It writes each object's envelope
from the batch's resource attributes `com.splunk.index`, `com.splunk.source`,
`com.splunk.sourcetype`, and `host.name`, so stamp them upstream with a `set` component, as
`splunk-hec-send.yaml` does. An event without them takes the token's defaults in Splunk. An index
the token isn't allowed to write fails the request with `400` code 7, and the objects from the
bad one on aren't indexed; keep the `index` you stamp in the token's allowed list. Every other
resource and event attribute goes out as an indexed field in `fields`, flattened to dotted keys.

`splunk_hec_in` puts the same four values on the resource, so a relay keeps them.

### Acknowledgment is off by default, and your Splunk Cloud stack may not offer it

A `200` from HEC means received, not indexed. `ack: true` makes `splunk_hec_out` poll
`/services/collector/ack` until Splunk confirms every request of a batch, or fails the batch after
`ack_timeout` (30s by default). It needs a token with indexer acknowledgment (`useACK`) on.
Splunk Enterprise offers it. Splunk documents it on Splunk Cloud only for the Firehose path, but a
Splunk Cloud 10.5.2605.9 trial stack offered "Enable indexer acknowledgment" on its tokens and
acknowledged `splunk_hec_out`'s requests with none timed out, so check the token settings on
your stack. Against a token without it, each request counts as delivered on its `200`, counted
`logit.output.acks{result="unsupported"}` with a warning, so turning `ack` on against a stack
that doesn't offer it changes nothing but that counter.

### A `useACK` token needs a channel on every request

A token with `useACK` on answers `400` to any request without a `X-Splunk-Request-Channel` header:
code 10 on Splunk Enterprise, and code 28 on Splunk Cloud, whose text adds that several indexers
need sticky-session load balancing. `splunk_hec_out` sends one per-sink channel on every request
whether `ack` is on or not, so it works against both token kinds. If you put another HEC client in
front of a `useACK` token, it needs a channel too. `splunk_hec_in` requires no channel on any route.

### Size caps

`splunk_hec_out` cuts each batch into requests of at most `max_body_bytes` (2 MiB by default,
before compression). An event larger than that alone is dropped, counted
`logit.output.records.dropped{reason="oversize"}`. Keep `max_body_bytes` under the receiver's
`limits.conf [http_input] max_content_length`: Splunk Enterprise 10.4.3 allows 838,860,800 bytes,
older releases 1,000,000. Splunk documents `413` for a body over it (the run didn't send
one), and `splunk_hec_out` doesn't retry a `413`. A Splunk Cloud 10.5.2605.9 trial stack accepted
bodies up to 5,242,881 bytes and refused 6,000,000 and above with `400` code 6 naming object 0,
not `413`. Over that cap, `splunk_hec_out` drops the body's first object as `invalid_event` and
resends the rest. The 2 MiB default sits under it.

`splunk_hec_in` caps a request at `max_request_bytes` (5 MiB by default), both as sent and after
gzip decompression, and answers `413` past it. Raise it if a client sends larger bodies.

### A `503` defers a client's data, and can duplicate some of it

When the pipeline doesn't take a request's events within 5 seconds, `splunk_hec_in` answers `503`
code 9 with `Retry-After: 1` rather than holding the connection, counted
`logit.input.batches.dropped{reason="busy"}`. HEC clients retry a code 9, so this defers delivery
rather than losing it. A `/event` body that carries several envelopes decodes into one batch per
envelope, though, and a `503` after some of them were delivered makes the retry deliver those
again. Give the sinks behind `splunk_hec_in` a `buffer:` large enough to absorb a stall.

`splunk_hec_out` isn't duplicate-safe either: Splunk indexes a resent event twice, and one batch
can be several requests. The default posture is at-most-once, so a `5xx` or a timeout drops the
batch; `buffer: {delivery: at_least_once}` retries it and accepts duplicates.

### One malformed event costs only itself

A `400` code 6 names the first object Splunk couldn't parse in `invalid-event-number`, counting
from 0. Splunk Enterprise 10.4.3 indexed every object before it and none from it on. So
`splunk_hec_out` drops that one object, counted
`logit.output.records.dropped{reason="invalid_event"}`, and resends the objects after it, once. A
second code 6 on the resend is permanent. The other per-object rejections (7, 12, 13, and 15) are
permanent: Splunk indexes the objects before the bad one and none from it on, and the rest of the
batch is dropped with them. Code 7 names the object after the bad one. `splunk_hec_out` doesn't write the shapes behind codes
12, 13, and 15 (a missing or blank `event`, a nested `fields` value), which leaves code 7, an
index the token can't write.

### `/raw` bodies are split into lines, and Splunk's line breaking doesn't run again

`splunk_hec_in` turns a `/raw` body into one log per line, stamped with the time it arrived, with
the envelope from the query string. Splunk would instead apply the sourcetype's `props.conf` line
breaking and timestamp extraction. `splunk_hec_out` always sends `/event` with an explicit `time`,
so Splunk applies neither to what `logit` relays: a multi-line event a `/raw` client sent stays
split into its lines. Merge them upstream of the sink with a `regex` or `lua` stage, or send that
client's data to Splunk directly.

### No S2S: a universal forwarder can't point at `logit`

A universal forwarder speaks only S2S, which has no public specification, and `logit` has no S2S
listener. Two outputs a heavy forwarder has reach `logit` instead:

- `outputs.conf [syslog]` into `syslog_in`, RFC 3164 over UDP or TCP;
- `[tcpout]` with `sendCookedData = false`, which writes each event's raw text followed by one LF
  and nothing else. `logit` has no plain-lines listener to receive it, and an event with an
  embedded newline arrives as two lines.

`docs/known-gaps.md`'s "Splunk" section tracks both, and Edge Processor's HEC destination.

## Credentials

Take `splunk_hec_out`'s `token` and `splunk_hec_in`'s `tokens` from the environment with
`!env SPLUNK_HEC_TOKEN`, as every example does. Neither component logs a token or keeps it on an
event, and both reject a token with leading or trailing whitespace at startup.

- **`splunk_hec_in` with `tokens` empty accepts any request**, whatever token it carries. Set
  `tokens` to the ones your clients send. A token in the query string (`?token=`) is always
  refused with `400` code 16. A token is a shared secret, not transport security: add `tls:`
  before binding beyond loopback. Splunk serves HEC over HTTPS by default, so a client configured
  with an `https://` URL needs `tls:` on the listener.
- **`splunk_hec_out`'s `endpoint`** is the base URL ending in `/services/collector`: `:8088` on
  Enterprise. Splunk documents `https://http-inputs-<stack>.splunkcloud.com/services/collector`
  for Splunk Cloud; on a trial stack that name doesn't resolve, and HEC is
  `https://<stack>.splunkcloud.com:8088/services/collector`. A Splunk Enterprise HEC with its
  default self-signed certificate needs `tls: {ca_file: ...}` naming that CA. The trial stack
  presents that same default certificate (`CN=SplunkServerDefaultCert`), whose name doesn't match
  the host, so it needs `tls: {insecure_skip_verify: true}`. Whether a paid stack presents a
  public certificate isn't verified. `/services/collector/health` answers without a token, so it
  can check the endpoint before a token is set up.

## What's verified

The codec and `splunk_hec_in` were checked against real traffic recorded from four HEC clients:
the OTel Collector contrib 0.161.0 `splunk_hec` exporter (logs, `/raw`, spans, and gauge, sum, and
histogram metrics in both forms), Docker 29.8.1's `splunk` log driver in each `splunk-format`,
SC4S 3.40.0, and splunk-library-javalogging 1.11.11
([`testdata/interop/splunk/`](../testdata/interop/splunk/README.md)). Every recorded request
decodes and gets a `2xx` from `splunk_hec_in`, including the Docker driver's `OPTIONS` check.

`script/splunk-interop` then ran both components against Splunk Enterprise 10.4.3
([`tools/splunk-interop/README.md`](../tools/splunk-interop/README.md)):

- `splunk_hec_out` delivered logs with indexed fields, every metric kind under `expand` (queried
  with `mstats`, including `histperc` over the expanded histogram), and span events searchable by
  every member;
- a `useACK` token acknowledged every request `splunk_hec_out` sent with `ack: true`;
- the recorded corpus, replayed into `splunk_hec_in` and relayed by `splunk_hec_out`, arrived in
  Splunk from every producer;
- probes settled what Splunk does with gzip and per-object-invalid bodies, read its
  `max_content_length` from `limits.conf` over REST, and settled `metric_type`, 1,000 dimensions, `OPTIONS`, and acknowledgment.

It ran the same legs and probes against a Splunk Cloud Platform 10.5.2605.9 trial stack
(`SPLUNK_INTEROP_TARGET=cloud`). A trial has no REST API, so each leg's arrival was confirmed by
searching in Splunk Web. Every leg landed as on 10.4.3, acknowledgment included, and the probes
found the endpoint, certificate, channel, and body-cap differences above. Edge Processor isn't
provisioned on the trial stack, and Ingest Processor has no destination that reaches `logit`: it
sends to Splunk indexes, S3, and Observability Cloud.

Not verified: the Observability Cloud leg (`fixtures/splunk-observability.yaml`), since no trial
org was run; a paid Splunk Cloud stack's `http-inputs-` endpoint and its certificate; Vector's HEC
sinks and an Edge Processor as clients; and any Splunk Enterprise release other than 10.4.3.
`docs/known-gaps.md`'s "Splunk" section lists everything else that isn't built or isn't
verified.
