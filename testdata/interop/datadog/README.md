# Datadog interop fixtures

Real traffic from a real Datadog Agent, a real dd-trace tracer, and a real DogStatsD client,
captured verbatim by `tools/record-fixtures/raw_capture.py` with no decompression, parsing, or
re-encoding. `logit`'s Datadog codecs and listeners were written from Datadog's docs, the Agent's
source, and `agent-payload`'s protos; these fixtures check that reading against what the software
puts on the wire. Nothing here came from, or went to, Datadog's backend: every Agent ran with the
fake key `logit-record`, every URL it had pointed at the capture sink, and its DNS pointed at an
unrouted address, so anything it tried on its own couldn't resolve a Datadog host.

To regenerate, run `script/record-fixtures datadog` (or one of `datadog-agent`,
`datadog-tracer`, `datadog-dogstatsd-unix`). The script's header comments say what each producer
runs. See `../README.md` for the corpus-wide rules.

## Fixtures

HTTP captures are two files per request: the body as sent (`.bin`) and a `.headers` sidecar
holding the method, the path, and every request header, lowercased. The route is in the sidecar's
`path:`, the body form in its `Content-Type`, and the compression in its `Content-Encoding`, so a
replay test reads all three from the sidecar. A `GET` has an empty `.bin`. Socket captures are
`.raw`: one file per datagram, or one per stream connection with its length prefixes intact.

| Files | Producer | Invocation | Captured | Construct exercised |
|---|---|---|---|---|
| `dogstatsd-unix-00{0..8}.raw` (9 files, 1,949 B) | `datadog` 0.54.0 (`DogStatsd`), pip-installed into `python:3.12-slim` (Python 3.12.14) | `python_dogstatsd_producer.py --url unix:///var/run/datadog/dsd.socket`, `DD_EXTERNAL_ENV` set, to `raw_capture.py --proto unix` | 2026-09-24 | One call per datagram, in the producer's order: a counter, a gauge (`cardinality="high"`), a histogram, a distribution, a set, a timer, a gauge with an explicit timestamp, an event (`cardinality="orchestrator"`), and a service check. Every line carries `\|c:in-<inode>` (the client's own container detection), `\|card:`, and, on metric lines, `\|e:<DD_EXTERNAL_ENV>` |
| `dogstatsd-unix-stream-000.raw` (1,985 B) | Same | `... --url unixstream:///var/run/datadog/dsd-stream.socket --modes unbuffered,buffered`, to `raw_capture.py --proto unix-stream` | 2026-09-24 | The same nine calls on one connection, unbuffered: nine frames, each a 4-byte little-endian length and one `LF`-terminated line |
| `dogstatsd-unix-stream-001.raw` (1,961 B) | Same | Same run's second connection | 2026-09-24 | The same nine calls buffered: three frames, the first packing six lines |
| `tracer-info-000.*` (191 B) | `ddtrace` 4.15.2 (libdatadog 9.0.0 underneath) and `flask` 3.1.3, same image | `ddtrace-run python3 python_ddtrace_app.py`, `DD_TRACE_AGENT_URL=http://capture:8126`; the sink answers `/info` with `tools/record-fixtures/datadog-trace-info.json`, which a test holds equal to `datadog_trace_in`'s own document | 2026-09-24 | The tracer's `GET /info` |
| `tracer-v0-5-traces-000.*` (7,550 B) | Same | Same run: tracer defaults | 2026-09-24 | Three Flask requests (one answering `500`) as `/v0.5/traces`, the tracer's default form: `X-Datadog-Trace-Count: 3`, the `Datadog-Meta-*` headers, `Datadog-Entity-ID`, `Datadog-External-Env`, `Datadog-Client-Computed-Top-Level` |
| `tracer-v04-info-000.*` (191 B) | Same | `DD_TRACE_API_VERSION=v0.4`, `DD_TRACE_STATS_COMPUTATION_ENABLED=true`, `RECORD_PATHS=/orders/42`; `/info` answered with the same document but `client_drop_p0s: true` | 2026-09-24 | The second run's `GET /info` |
| `tracer-v04-v0-4-traces-000.*` (3,882 B) | Same | Same run | 2026-09-24 | One request as `/v0.4/traces`, `Datadog-Client-Computed-Stats: true` |
| `tracer-v04-v0-6-stats-000.*` (1,210 B) | Same | Same run | 2026-09-24 | The tracer's own client stats, `/v0.6/stats`: `Lang` and `TracerVersion` empty in the payload, sent only as headers; sketches on libdatadog's mapping (gamma 1.015625, an index offset) |
| `agent-info.json` (2,741 B) | Datadog Agent 7.83.3 (`docker.io/datadog/agent:7`; `agent version` reports commit `8c639c92581`, serialization v5.0.207) | `curl --unix-socket /var/run/datadog/apm.socket http://localhost/info` inside the Agent container | 2026-09-24 | A real Agent's `/info`, the document `datadog_trace_in`'s is held to field by field |
| `agent-api-v2-series-00{0..4}.*` (6,954 B) | Same Agent | The Agent with `DD_DD_URL`, `DD_APM_DD_URL`, and `DD_LOGS_CONFIG_LOGS_DD_URL` at the capture sink, its whole `conf.d` replaced by one log tail (`datadog-agent-logs.yaml`), all three DogStatsD listeners and the APM socket enabled, fed the nine DogStatsD calls over UDP, the datagram socket, and the stream socket, then the Flask app over the APM socket | 2026-09-24 | `/api/v2/series`, protobuf, zstd: the Agent's own metrics, the Agent Data Plane's probe, the three `\|T` gauges (sent at once, unaggregated), and the aggregated flushes carrying the clients' counter (as a `rate`), gauge, set, and histogram and timer aggregates |
| `agent-api-beta-sketches-000.*` (423 B) | Same | Same run | 2026-09-24 | `/api/beta/sketches`, protobuf, zstd: the distribution, one sketch of three values |
| `agent-api-v1-check-run-00{0..2}.*` (1,127 B) | Same | Same run | 2026-09-24 | `/api/v1/check_run`, JSON, zstd: `datadog.agent.up`, and the client's service check once per transport |
| `agent-intake-001.*`, `agent-intake-003.*` (891 B) | Same | Same run | 2026-09-24 | `/intake/`, JSON, zstd: the Agent's startup event, and the client's events once per transport, in the Agent's `events` envelope. The run's other two `/intake/` requests, host metadata and a process snapshot, aren't kept (see [Privacy](#privacy)) |
| `agent-api-v2-logs-00{0..1}.*` (995 B) | Same | Same run | 2026-09-24 | `/api/v2/logs`: the logs sender's `{}` connectivity check (uncompressed), then the three tailed lines, JSON, zstd |
| `agent-api-v0-2-traces-000.*` (3,493 B) | Same | Same run | 2026-09-24 | `/api/v0.2/traces`: the Flask traces as an `AgentPayload`, protobuf, zstd, each root marked `_top_level` |
| `agent-api-v0-2-stats-000.*` (977 B) | Same | Same run | 2026-09-24 | `/api/v0.2/stats`, msgpack, gzip: stats the Agent computed, one group per request |
| `agent-api-v1-validate-000.*`, `agent-api-v2-validate-000.*`, `agent-health-000.*` (417 B) | Same | Same run | 2026-09-24 | The probes: the key check `GET /api/v1/validate?api_key=...` with no key header, the trace agent's `GET /api/v2/validate` with one, and `GET /_health` |

The Agent's flushes run on its own 15-second clock, so which series request carries the
clients' metrics depends on timing. The capture takes five series requests, four `/intake/`, and
three service-check requests, and `tools/record-fixtures/check_datadog_agent_capture.py` fails the
run unless every construct the tests read is somewhere in what it keeps.

The whole directory is 36,937 bytes without this README, inside `../README.md`'s budget. One
choice keeps it there: a Flask request is about ten spans, so the `tracer-v04` run serves one
request instead of three.

The run pulls `docker.io/datadog/agent:7` and `pip install`s current `datadog`, `ddtrace`, and
`flask`, so a re-record picks up whatever is current. Each producer prints the version it
resolved; update the table from that output.

## Privacy

The recorder keeps nothing that describes the machine it ran on. Agent 7.83 posted host metadata
to `/intake/`, and an Agent can also use `/api/v2/host_metadata`. It holds the host's hardware UUID, its disk and LUKS device UUIDs
under `/dev/mapper/`, the CPU model, the kernel build, and its memory. That identifies one physical
machine, and no test reads it. So `raw_capture.py --discard` answers `/api/v2/host_metadata`
without writing it, and `check_datadog_agent_capture.py` deletes every `/intake/` body with no
`events` key, which also drops the Agent's process snapshot. The check then fails the run if any
kept body, decompressed, still contains `/dev/`, `"uuid"`, `cpu_cores`, `model_name`,
`kernel_version`, or `luks`. It runs even when the capture fails, because the capture writes
straight into this directory.

## What this settled

Each item below was UNVERIFIED in `docs/plans/datadog-relay.md` or ADR
`datadog-agent-and-intake-relay` before these captures.

- **The DogStatsD stream socket frames each packet as a 4-byte little-endian length.** The payload
  is one datagram's worth of `LF`-terminated lines, and a buffered client packs several into one
  frame (`dogstatsd-unix-stream-*.raw`). This matches `FramingMode::LengthPrefixedLe` and
  `statsd_out`'s `unix_stream` writer.
- **A metric line's optional segments come in the order `|#tags|c:|e:|card:|T`**
  (`dogstatsd-unix-006.raw`), which is the order `statsd_out` writes.
- **The `datadog` client writes a service check's `c:` and `card:` after `m:`, and the Agent ends
  `m:` at the next `|`.** The client sent `...|m:slow upstream|c:in-...|card:low`
  (`dogstatsd-unix-008.raw`), and the Agent reported the message as `slow upstream`
  (`agent-api-v1-check-run-*.bin`). `statsd_in` read `m:` to the end of the line, which turned
  those segments into message text and lost the container id and cardinality. **Changed:**
  `statsd_in` now ends `m:` at the next `|` as the Agent does, and `statsd_out` substitutes `_`
  for a `|` in a message, since no receiver can read one.
- **The client sends no `e:` on an event or a service check**, only on metric lines
  (`dogstatsd-unix-007.raw`, `-008.raw`).
- **The Agent's DogStatsD and APM sockets are both mode `0722`**, owned by root: the recording
  runs `ls -ln` and `stat` inside the Agent container and prints them. **Changed:**
  `datadog_trace_in` makes its socket `0722` too, instead of `0666`. Connecting needs only write
  permission, so any user can still connect.
- **The Agent's APM socket serves plain HTTP/1.1 with `Host: localhost`**: `agent-info.json` came
  from a `curl` request of that shape. The same day, a one-off run relayed the Flask app's traces
  and the DogStatsD client's calls through `logit` to the same Agent image's sockets
  (`datadog_trace_in` -> `datadog_trace_out` `socket:`, `statsd_in` -> `statsd_out`
  `transport: unix_stream` and `transport: unix`). The Agent forwarded every span, metric, event,
  and service check to the capture sink. That run isn't a recorded producer, because what it
  checks is `logit`'s output, not a fixture.
- **A dd-trace tracer's `/info` must type-check against the Agent's.** libdatadog, under
  dd-trace-py 4.x, rejected `datadog_trace_in`'s document over `"redis": false`, where the Agent
  sends an object (`agent-info.json`). It then ran as if no Agent had answered, and never computed
  client stats. **Changed:** the document's `redis`, `valkey`, and `memcached` entries are objects,
  it carries `sql_obfuscation_mode`, `filter_tags`, and `filter_tags_regex`, and a test holds every
  field to the recorded Agent's type.
- **A libdatadog tracer computes client stats only when `/info` says `client_drop_p0s: true`.**
  Against `datadog_trace_in`'s document, which says `false` so that every span reaches the relay,
  the tracer sent spans and no stats (`tracer-*` captures, and a one-off run that waited 40
  seconds). With `true`, it sent `/v0.6/stats` (`tracer-v04-v0-6-stats-000`). So behind
  `datadog_trace_in` the downstream Agent computes the stats.
- **dd-trace-py 4.15.2 retries a `503` on its trace route five times, waiting 100 ms and
  doubling, then drops the payload.** This was a one-off probe of dd-trace-py 4.15.2 against an
  endpoint that answered its trace route `503`, not a recorded fixture, and its exact invocation
  wasn't kept. The endpoint saw six identical requests over about 3 seconds (the first try and
  five retries), then the tracer logged `failed to send, dropping 1 traces`. ADR decision 11
  assumed no retry. The mechanism is unchanged (a `503` still leaves the batch undelivered to
  every consumer, so a retry can't duplicate it); decision 11 now gives the window within which a
  `503` defers delivery and past which it's loss.
- **The tracer's client stats name the tracer only in headers.** `Lang` and `TracerVersion` are
  empty in the payload and present as `Datadog-Meta-Lang` and `Datadog-Meta-Tracer-Version`
  (`tracer-v04-v0-6-stats-000.*`). The Agent fills a stats payload's empty `Lang`,
  `TracerVersion`, and `ContainerID` from those headers. **Changed:** `datadog_trace_in` does the
  same on `/v0.6/stats`, which it previously ignored all headers on.
- **Tracers send `Datadog-External-Env`**, the tracer's copy of `DD_EXTERNAL_ENV`. **Changed:**
  `datadog_trace_in` carries it as `datadog.tracer.external_env` and `datadog_trace_out` sends it
  back, as it does `Datadog-Entity-ID`.
- **A 7.83 Agent sends three probes the intake stand-in didn't answer.** The key check is
  `GET /api/v1/validate?api_key=<key>` with no `DD-API-KEY` header, so `datadog_in` with
  `api_keys` set answered it `403`. The trace agent fetches `GET /api/v2/validate` (with the
  header), and something asks `GET /_health`; both got `404`. **Changed:** `datadog_in` serves all
  three, and reads `?api_key=` on them.
- **The Agent's logs sender posts `{}` before its first batch**, uncompressed
  (`agent-api-v2-logs-000.*`). The codec counted it as a log skipped for having no message.
  **Changed:** a bare empty object is no log and isn't counted.
- **The Agent compresses series, sketches, service checks, `/intake/`, logs, and traces with
  zstd, and stats with gzip**, each one zstd frame. `datadog_in` decodes every recorded body
  (`crates/logit-cli/tests/datadog_in_round_trip.rs`).
- **DogStatsD events travel in `/intake/`'s `events` envelope, grouped by source type**, alongside
  host metadata and a process snapshot on the same route, which the recorder doesn't keep
  (`agent-intake-*`).
- **A redirected Agent sends v2 series, not v3.** With `dd_url` at a non-Datadog host, every
  series request went to `/api/v2/series`. This is one data point for how the Agent classifies a
  URL as Datadog's (`use_v3_api.series.enabled: datadog_only`), not the rule itself.
- **dd-trace-py 4.15 sends `/v0.5/traces` by default, and `/v0.4/traces` with
  `DD_TRACE_API_VERSION=v0.4`**, both msgpack with `POST`. It reads `/info` three times at
  startup. It warns that `logit/0.1.0` is an unsupported Agent version and so disables uploads
  from Dynamic Instrumentation, which `datadog_trace_in` would discard anyway.

## Tests that consume these fixtures

- `crates/logit-proto/tests/datadog_interop.rs` replays every HTTP capture through its route's
  decoder: nothing `Malformed`, skipped, degraded, or diagnosed, then the codec's fixed point, then
  the facts above (metric names and kinds, the sketch's count and sum, the events' titles, the
  check's message, the log lines, the span tree and its `_top_level` marks, the stats groups).
- `crates/logit-inputs/src/statsd.rs`'s `interop_fixture_unix_*` tests decode every datagram and,
  through the `unix_stream` framer, every stream frame, and check each construct's origin fields.
- `crates/logit-inputs/src/datadog_trace.rs` holds `/info` to `agent-info.json` and to the
  recording's reply file, and maps the recorded tracer headers.
- `crates/logit-cli/tests/datadog_in_round_trip.rs` and `datadog_trace_in_round_trip.rs` replay
  every recorded request over a socket, with its recorded headers, and expect the route's `2xx`.
- `crates/logit-cli/tests/datadog_pair_round_trip.rs` and `datadog_trace_pair_round_trip.rs`
  relay a recorded series request and the recorded tracer requests through each pair unchanged.

## What isn't covered here (yet)

- **Anything Datadog's intake decides.** Nothing here reached Datadog. The plan's W7b trial-org
  run settled most of it (sketches and traces from a sender that isn't an Agent, zlib on
  distribution points, the size limits, series dedupe), recorded in
  `docs/plans/datadog-relay.md` and ADR `datadog-agent-and-intake-relay` rather than as fixtures. Whether the intake
  takes a stats sketch that isn't on gamma 1.0202 is still open (`docs/known-gaps.md`).
- **v0.7 traces, a `PUT`, and a second tracer language.** dd-trace-py sends neither v0.7 nor
  `PUT`; dd-trace-java and dd-trace-go would. The codec's fixed-point tests cover both forms.
- **v1 series, distribution points, and the public API's JSON forms.** A current Agent sends
  none of them.
- **A check that sets a `device`.** The Agent ran no checks, so no series carries one, and the
  v2 serializer's handling of a v1 `device` stays as the codec documents it.
- **A real tracer's log-correlation fields** (`dd.trace_id`, `dd.span_id`) for `trace_context`'s
  `format: datadog`. The Flask app logs nothing through an injected formatter. The W7b run lifted
  one real dd-trace-py 4.15.2 line, not kept here.
- **`Datadog-Send-Real-Http-Status: 1`**, which the tracer sends on every trace request to ask
  the Agent for real status codes. `datadog_trace_in` always answers with real ones, and
  `datadog_trace_out` doesn't carry the header on.
