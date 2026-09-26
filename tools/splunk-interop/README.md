# splunk-interop

`script/splunk-interop` checks `splunk_hec_out` and `splunk_hec_in` against a real Splunk
Enterprise container, or a Splunk Cloud stack in [Splunk Cloud mode](#splunk-cloud-mode), then
probes that Splunk directly for what
[`docs/plans/splunk-relay.md`](../../docs/plans/splunk-relay.md) listed as unverified. It prints
one row per leg and one per probe. [What the run showed](#what-the-run-showed) records a Splunk
Enterprise run and a Splunk Cloud run; the plan's "Settled by W5" and "Settled by the Cloud run"
sections and ADR `splunk-hec-relay`'s two amendments carry the decisions they settled.

The script runs on the host and drives docker (`$DOCKER`, `sudo docker` by default), like
`script/victoria-interop` and `script/record-fixtures`. It isn't part of `script/cibuild`, and no
test depends on it running. Splunk needs several GB of RAM and two to three minutes to start.

## What it runs

`compose.yaml` starts one stack under the compose project `splunk-interop`, on the network
`splunk-interop-net`, with no host ports. Every credential is a fixed dummy for a throwaway
container; `compose.yaml`'s header lists them.

| Service | Image | Role |
|---|---|---|
| `splunk` | `splunk/splunk:10.4.3` | Splunk Enterprise, HEC over plain HTTP on `:8088`, the default token `splunk_hec_token` |
| `splunk-init` | `curlimages/curl:8.22.0` | One-shot REST setup on `:8089`: a metrics index `metrics`, event indexes `osnix` and `tcpout_probe`, a `useACK` token `ack`, and the `[tcpout]` probe's output group and token, then a restart for the output group to load |
| `rawcap` | `python:3.12-slim` | `tools/record-fixtures/raw_capture.py --proto tcp`, the `[tcpout] sendCookedData=false` destination |
| `logit-<leg>` | `logit:splunk-interop`, built from the current tree | one per `logit-<leg>.yaml` |
| `replay` | `python:3.12-slim` | `replay.py`: every request in `testdata/interop/splunk/` into the `hec-relay` leg, once |

The legs:

| Leg | Config | What it sends |
|---|---|---|
| `hec-logs` | `logit-hec-logs.yaml` | a warn-level log per second with a trace reference and a nested attribute, sourcetype `logit:test`, into `main` |
| `hec-metrics` | `logit-hec-metrics.yaml` | every metric kind under `multi_value: expand` into `metrics`: gauge, cumulative and delta sum, samples, set members, histogram, summary, exponential histogram, and, through `aggregate`, a distribution and a set |
| `hec-spans` | `logit-hec-spans.yaml` | a server span per second with an error status and a span event, sourcetype `logit:span` |
| `hec-ack` | `logit-hec-ack.yaml` | the logs leg on the `ack` token with `ack: true` |
| `hec-relay` | `logit-hec-relay.yaml` | `splunk_hec_in` fed by `replay`, relayed by `splunk_hec_out` into Splunk: the tee topology |

Every config passes `logit validate`: `script/validate` and the
`every_shipped_config_loads_and_validates` test both cover `logit-*.yaml` here.

## A run

1. Builds `logit:splunk-interop` from `Dockerfile` (set `SPLUNK_INTEROP_SKIP_IMAGE=1` to reuse
   it) and validates every leg config with it.
2. Brings the stack up with `docker compose up --wait`, which waits on Splunk's health check
   (`GET /services/collector/health`), on `splunk-init`, and on each `logit` service's `logit
   ready`, then starts `replay`.
3. Lets traffic flow for `SPLUNK_INTEROP_WINDOW` seconds (default 60, at least 75 in cloud
   mode, which outlasts the sink's 60 s retry budget for a connection it can't open), then
   one telemetry interval more.
4. Copies every service's log into the run directory and runs `check.py` in a
   `python:3.12-slim` container on the stack's network. `check.py` searches Splunk over REST
   (`/services/search/jobs/export`, `mstats`, `mcatalog`, `tstats`) for each leg, an event
   search bounded by index time to what arrived after the legs started (`mstats` and `mcatalog`
   return nothing under that bound, so a metrics query is unbounded), then runs the probes: gzip,
   `max_content_length`, the HEC endpoint's TLS and `/health`, the body-size cap, the code 6
   batch semantics, `metric_type`, the dimension count, `OPTIONS` and `Set-Cookie`,
   acknowledgment, and `[tcpout]` framing.
5. Tears the project down (`down -v --remove-orphans`) on every exit.

A leg's row is `PASS`, `GAP` (it arrived, with a difference recorded below), `SENT` (not
searched, see [Splunk Cloud mode](#splunk-cloud-mode)), `SKIP` (the target can't run it), or
`FAIL`; a probe's is `INFO`, what Splunk answered, or `SKIP`. The script exits 1 on any `FAIL`.

A run writes `perf/results/splunk-interop/<timestamp>/` (gitignored, or under
`SPLUNK_INTEROP_OUT`): `results.md` and `results.json`, `search.spl` with every SPL query the
legs and probes ran, `provenance.txt` with the target and the image tags, `logs/<service>.log`,
`replay.log`, one `<leg>-telemetry.log` per leg (`hec-logs-telemetry.log`,
`hec-metrics-telemetry.log`, `hec-spans-telemetry.log`, `hec-ack-telemetry.log`, and
`hec-relay-telemetry.log`: the sink's own telemetry, which every leg config taps with
`internal`), and `tcpout/tcpout-000.raw`.

## Splunk Cloud mode

```sh
SPLUNK_INTEROP_TARGET=cloud script/splunk-interop
```

runs the same five legs and the same probes against a Splunk Cloud Platform stack. The legs'
`splunk_hec_out` endpoint, token, and TLS verification come from the environment, which
`compose.yaml` defaults to the local stack, so no config changes between the two modes. In cloud
mode the script starts only the `logit-*` legs and `replay`, with `--no-deps`: `splunk`,
`splunk-init`, and `rawcap` never start, and nothing on the stack is set up for the run.

The stack's details come from an env file, `perf/results/splunk-cloud.env` by default
(`SPLUNK_INTEROP_CLOUD_ENV` names another). It holds credentials, so the script refuses to run
unless git ignores it; `perf/results/` is ignored by the repository. One `KEY=value` per line,
no quotes (`docker run --env-file` keeps them as part of the value). The script also refuses a
file without `SPLUNK_INTEROP_HEC_URL` or `SPLUNK_INTEROP_HEC_TOKEN`.

| Key | What it is |
|---|---|
| `SPLUNK_INTEROP_HEC_URL` | The stack's HEC base URL, ending in `/services/collector`, for example `https://<stack>.splunkcloud.com:8088/services/collector`. Required |
| `SPLUNK_INTEROP_HEC_URL_ALT` | A second HEC URL for the endpoint probe to compare, such as the stack's `http-inputs-<stack>` host on `:443`. Optional |
| `SPLUNK_INTEROP_HEC_TOKEN` | A HEC token that may write `main`, and `metrics` and `osnix` for the metrics and relay legs. Required |
| `SPLUNK_INTEROP_ACK_TOKEN` | A HEC token with indexer acknowledgment on. Empty or absent, the `hec-ack` leg doesn't start and its row and the ack probe are `SKIP` |
| `SPLUNK_INTEROP_HEC_INSECURE` | `true` turns off certificate verification in the legs, for a trial stack's self-signed HEC certificate. Default `false`, and an empty value reads as `false` |
| `SPLUNK_INTEROP_SEARCH` | `none` (the default in cloud mode) searches nothing; `rest` searches over the REST API as in local mode |
| `SPLUNK_INTEROP_API_URL` | The REST API base URL, for `SPLUNK_INTEROP_SEARCH=rest` |
| `SPLUNK_INTEROP_API_AUTH` | The whole `Authorization` header value for the REST API, `Basic …` or `Bearer …` |
| `SPLUNK_INTEROP_STACK` | The stack name, scrubbed from the results as `<stack>` |

Under `SPLUNK_INTEROP_SEARCH=none`, a leg is `SENT` rather than `PASS` when the sink's
telemetry shows N requests answered `2xx` for M records with nothing dropped; arrival is
unconfirmed by search. `SENT` also needs no rejection in the sink's log; for `hec-relay`, every
replayed request answered `2xx`; and for `hec-ack`, every request acknowledged. Otherwise the leg
is `FAIL`, with the request counts by class (`network_error` included), the dropped records by
reason, the batches dropped, and the retries in its detail. The row carries the SPL that would
confirm arrival. `search.spl` collects those
queries and the probes', each under a `# <leg or probe>` comment, for a search pass by hand or in
a browser afterward: run each over all time, adding `_index_earliest` set to the epoch in the
file's header to a `search` query, since the relay leg's events carry their recorded timestamps.
The probes still post and record what the stack answered, with `not searched` where they would
have checked indexing. `max_content_length` needs the REST API and is `SKIP`; `[tcpout]` needs
the local `rawcap` and is `SKIP` in cloud mode.

The probes that matter most for a stack:

- **Endpoint**: `GET /services/collector/health` with certificate verification on and off, the
  leaf certificate's subject and issuer, and whether `/health` needs a token, for
  `SPLUNK_INTEROP_HEC_URL` and `SPLUNK_INTEROP_HEC_URL_ALT`. Over `http://` it records `plain
  http` and skips the TLS checks.
- **Body cap**: `/event` bodies of 999,000, 1,000,001, 1,048,577, and 2,000,000 bytes, then two
  2,000,000-byte bodies gzipped, one incompressible and one compressing to a few KB, each posted
  once with its status, reply, `Content-Type`, `Retry-After`, and whether it was indexed, or the
  transport error when the receiver closes the connection before the reply is read. It shows
  whether a receiver's cap counts the compressed or the uncompressed bytes.
- **`Set-Cookie`**: the cookie names on an `/event` reply, values redacted: a load balancer in
  front of HEC can pin a channel's ack polls to one indexer.

A probe that posts to `metrics` records a `400` code 7 when the token may not write that index.

`provenance.txt` records `target: cloud (stack redacted)`. `results.md`, `results.json`, and
`search.spl` have `SPLUNK_INTEROP_STACK` and every configured token and credential replaced before
they're written. The services' own logs under `logs/` aren't scrubbed: a sink's diagnostic can
quote the endpoint URL.

## Cleanup and a shared daemon

Everything the script creates belongs to the `splunk-interop` project, so cleanup can't touch
another session's containers. The project name is fixed, so one run at a time per daemon: the
script refuses to start while the project has containers, and prints the `down` command for a
stack a crashed run left behind.

## What the run showed

### Splunk Enterprise 10.4.3 (2026-09-25)

A run on 2026-09-25 against `splunk/splunk:10.4.3` (build `4174a2deda5d`), `logit` built from
this branch, `SPLUNK_INTEROP_WINDOW=60`. Every leg passed; the probe rows are what Splunk
answered. The recording of the `[tcpout]` capture is described but not committed: see its row.

| Leg | Result | What Splunk held |
|---|---|---|
| `hec-logs` | PASS | Each log with `host`, `source`, and `sourcetype` from the resource; `otel.log.severity.text` `Warn` and `.number` `13`, `trace_id`, `span_id`, and the flattened `detail.stage` and `detail.ok` as indexed fields (`tstats` groups by them) |
| `hec-metrics` | PASS | 21 series: `gauge`, `sum_cumulative`, `sum_delta`, `samples_{count,sum,min,max}`, `set_members`, `histogram_{sum,count,bucket}`, `summary_{sum,count,0.5,0.99}`, `distribution_{count,sum,p50,p90,p99}`, and `set`; no exponential histogram. `metric_type` takes `Gauge`, `Histogram`, `Sum`, and `Summary`. The histogram's `_bucket` series carries `le` `0.1`, `1`, `10`, and `+Inf` with cumulative counts, and `` `histperc(0.5, c, le)` `` answers 2.8 |
| `hec-spans` | PASS | The span object, searchable by `trace_id`, `span_id`, `name`, `kind` `SPAN_KIND_SERVER`, `status.code` `STATUS_CODE_ERROR`, `status.message`, `start_time`, `end_time`, and `events{}.name`, with `service.name` as an indexed field |
| `hec-ack` | PASS | 69 requests acknowledged, none timed out or unsupported, 104 `/ack` polls; 71 events indexed |
| `hec-relay` | PASS | All 32 recorded requests answered `2xx` by `splunk_hec_in`, and every producer's events relayed: the Docker driver's 6, the Java appender's 9, SC4S's 3 lines (index `osnix`) and 4 own events, the exporter's 3 logs, 3 raw lines, and 6 spans, and its metrics `gen`, `gen_sum`, `gen_count`, `gen_bucket` |

| Probe | Splunk 10.4.3's answer |
|---|---|
| `max_content_length` | `limits.conf [http_input] max_content_length = 838860800` (800 MiB) |
| gzip | `Content-Encoding: gzip` on `/event` and `/raw`: `200`, indexed. `deflate`: `415` with an HTML body |
| code 6 | A syntax error in object 1 of 3: `400` `{"text":"Invalid data format","code":6,"invalid-event-number":1}`; object 0 indexed, objects 1 and 2 not. In object 0: `invalid-event-number` 0, nothing indexed |
| other per-object errors | A blank `event` (code 13), `fields` with a nested object (code 15), and an object with neither `event` nor `fields` (code 12), each in object 1 of 3: `400` naming 1, object 0 indexed, the rest not. An index the token doesn't allow in object 1 (code 7): `400` naming 2, object 0 indexed, the rest not |
| lenient cases | An object with `fields` and no `event`: `200`, skipped, the others indexed (as a metric when `fields` carry a measurement). An unknown envelope key in object 1 of 3: `200`, but only object 0 indexed; in a body's only object: `400` code 5 `No data` |
| metric forms | No `event` with a string measurement (SC4S's shape), and the single-metric `metric_name`/`_value` pair: both `200` and stored as metrics |
| `metric_type` | An ordinary dimension: `mcatalog values(metric_type)` lists it and `mstats … by metric_type` groups by it; a `Sum`'s value is stored as sent |
| dimensions | 200 and 1,000 dimensions on one metric event: `200`, and `mcatalog` sees all of them (201 and 1,001 with the probe's own) |
| `OPTIONS` | Every route: `200`, empty body, no token needed, `Allow: POST,OPTIONS` (`GET,HEAD,OPTIONS` on `/health`) |
| HTTP errors | Unknown path: `404` `{"text":"The requested URL was not found on this server.","code":404}`; `GET` on `/event`: `405` with the same body |
| `/raw` without a channel | On a token without `useACK`: `200` |
| `useACK` | No channel: `400` code 10. A new channel's first two requests: `{"text":"Success","code":0,"ackId":0}`, then `"ackId":1` (the key is `ackId`, ids count from 0 per channel). The id polled `true` within about a second; the same id polled on another channel: `false` |
| `[tcpout] sendCookedData=false` | Each event's `_raw` followed by one LF, nothing else: no header, no length, no metadata. An event with an embedded newline arrives as two lines; a JSON `event` as its JSON text; a `/raw` body's lines one each. Splunk forwarded its own logs from every index too, whatever `defaultGroup` (unset) and the `forwardedindex` filters (tried: only `tcpout_probe`) said, so the capture isn't committed: it is mostly Splunk's `_internal` and `_introspection` data |

### Splunk Cloud Platform 10.5.2605.9 (2026-09-26)

Two runs on 2026-09-26 with `SPLUNK_INTEROP_TARGET=cloud` against a Splunk Cloud Platform trial
stack (stack name redacted), which Splunk Web reports as `Splunk 10.5.2605.9`, `logit` built from
this branch, `SPLUNK_INTEROP_HEC_INSECURE=true`, and an ack token. A trial has no REST API (`:8089`
times out), so the runs searched nothing (`SPLUNK_INTEROP_SEARCH=none`) and every leg's row was
`SENT`: every `/event` request answered `2xx`, nothing dropped, and no ack `timeout` or
`unsupported`. Arrival was confirmed afterward by searching in Splunk Web with `search.spl`'s
queries. The tables record the second run: its counts come from a search bounded to what it indexed
(`_index_earliest` set to the run's start), and the field-level detail from the same search over the
first run's data. The two runs' probes answered identically. What Splunk held matched the 10.4.3 run
in every leg.

| Leg | Result | What Splunk held |
|---|---|---|
| `hec-logs` | PASS | 100 events, host `splunk-interop-logit`, source `splunk-interop`, `otel.log.severity.text` `Warn` and `.number` `13`, `detail.stage` `mint`, `detail.ok` `true`, and `leg` `hec-logs`, all as indexed fields (`stats` groups by them) |
| `hec-metrics` | PASS | 21 series `splunk_interop.*` with `metric_type` `Gauge`, `Histogram`, `Sum`, and `Summary`; the histogram's buckets `le` `0.1`=1, `1`=4, `10`=9, `+Inf`=10, and `` `histperc(0.5, c, le)` `` answers 2.8 |
| `hec-spans` | PASS | 100 span objects with `trace_id`, `span_id`, `name` `splunk-interop-span`, `kind` `SPAN_KIND_SERVER`, `status.code` `STATUS_CODE_ERROR`, `status.message` `boom`, `start_time`, `end_time`, `events{}.name` `retry`, and `service.name` `splunk-interop-spans` |
| `hec-ack` | PASS | 97 events indexed; the sink counted `acked=78 timeout=0 unsupported=0` over 117 `/ack` polls |
| `hec-relay` | PASS | Every producer: the Docker driver's events, the Java appender's 9 (`java:json`, `java:raw`, and `java:text`, 3 each), SC4S's 3 lines (`nix:syslog`, index `osnix`) and 4 own events, and the exporter's 3 logs and 6 spans; in the first run, the exporter's metrics `gen` (`Gauge` and `Sum`), `gen_bucket`, `gen_count`, and `gen_sum` |

| Probe | Splunk Cloud 10.5.2605.9's answer |
|---|---|
| endpoint | `https://<stack>.splunkcloud.com:8088/services/collector`; the `http-inputs-<stack>.splunkcloud.com` form doesn't resolve for this stack. With verification on: `certificate verify failed: self-signed certificate in certificate chain`; subject `commonName=SplunkServerDefaultCert, organizationName=SplunkUser`, issuer `organizationName=Splunk, commonName=SplunkCommonCA`. `/health` without a token: `200` `{"text":"HEC is healthy","code":17}` |
| body cap | 999,000, 1,000,001, 1,048,577, and 2,000,000 bytes uncompressed, and 2,000,000 bytes gzipped (incompressible, 1,514,967 bytes on the wire, and compressible, 2,009): each `200` and indexed. Posted by hand: 3 MB, 5 MB, and 5,242,881 bytes `200`; 6 MB, and 7, 8, 12, and 20 MB, `400` `{"text":"Invalid data format","code":6,"invalid-event-number":0}`, not `413`. The cap is between 5,242,881 and 6,000,000 bytes |
| gzip | `Content-Encoding: gzip` on `/event` and `/raw`: `200`, indexed. `deflate`: `415` with an HTML body |
| code 6 and other per-object errors | As on 10.4.3: codes 6, 7, 12, 13, and 15 name the same object, and the objects before it are the ones indexed |
| lenient cases | As on 10.4.3, `400` code 5 `No data` included |
| metric forms | Both `200` and stored as metrics |
| `metric_type` | `200`, stored as a dimension |
| dimensions | 200 and 1,000 dimensions: `200`, all of them stored (201 and 1,001 with the probe's own) |
| `OPTIONS` | As on 10.4.3 |
| HTTP errors | As on 10.4.3: `404` for an unknown path, `405` for `GET` on `/event`, both with the `404` body |
| `/raw` without a channel | On a token without `useACK`: `200`, indexed |
| `Set-Cookie` | None on an `/event` reply |
| `useACK` | The token settings offer "Enable indexer acknowledgment". No channel: `400` `{"text":"Data channel is missing. If you have multiple indexers, sticky session load balancers must be provisioned and client requests must be routed accordingly.","code":28}`, where 10.4.3 answers code 10. A new channel's first two requests: `{"text":"Success","code":0,"ackId":0}`, then `"ackId":1`. The id polled `true` after 1.3 s; the same id polled on another channel: `200` `{"acks":{"0":false}}` |
| `max_content_length` | SKIP: no REST API |
| `[tcpout] sendCookedData=false` | SKIP: cloud mode has no `rawcap` receiver or output group |

Neither Edge Processor nor Ingest Processor is provisioned on the trial stack: Data Management
shows only a link to request it, which takes a support case or the account team.
