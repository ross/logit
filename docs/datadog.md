# Datadog and `logit`

`logit` speaks Datadog's own protocols in both directions. It can send logs, metrics, traces,
events, and service checks to Datadog, straight to the intake API or through a local Datadog
Agent. It can also receive what Datadog-instrumented systems produce, losslessly, from either side
of an Agent: in front of the applications, where it answers dd-trace tracers and DogStatsD clients
as a local Agent would; and behind a fleet of Agents, where it answers them as Datadog's intake
would. Both stand-ins need nothing changed on the sender but a URL, which is what makes a migration
to or from Datadog a configuration change rather than an application change.

This doc helps you pick a topology and avoid the mistakes that lose data. Each component's
configuration and telemetry are in [`docs/deploying.md`](deploying.md):
[`datadog_in`](deploying.md#datadog_in-standing-in-for-datadogs-intake),
[`datadog_trace_in`](deploying.md#datadog_trace_in-standing-in-for-the-agents-apm-api),
[`datadog_out`](deploying.md#datadog_out-sending-straight-to-datadog),
[`datadog_trace_out`](deploying.md#datadog_trace_out-sending-to-an-agents-apm-api), and
`statsd_in`/`statsd_out` over the Agent's
[DogStatsD socket](deploying.md#statsd_in-dogstatsd-over-a-unix-socket).

Related docs:

- [ADR `datadog-agent-and-intake-relay`](adr/datadog-agent-and-intake-relay.md): the design
  decisions, including why Datadog-origin traces never go to Datadog as OTLP.
- [`docs/plans/datadog-relay.md`](plans/datadog-relay.md): the protocol survey, what Datadog
  accepts and emits, and how each Datadog concept maps onto `logit`'s event model.
- [`docs/known-gaps.md`](known-gaps.md)'s "Datadog" section: what isn't built or isn't verified.

## Topologies

Four topologies cover the ways `logit` sits next to Datadog. Each has a runnable example.

- **Direct to Datadog's API.** `logit` is the host collector and posts to Datadog's intake with no
  Agent in the path: [`examples/datadog-direct.yaml`](../examples/datadog-direct.yaml) runs
  DogStatsD through `aggregate` into `datadog_out`.
- **Through a local Agent.** `logit` hands data to an Agent on the host, which sends it on:
  [`examples/datadog-via-agent.yaml`](../examples/datadog-via-agent.yaml) sends OTLP traces and
  metrics to the Agent's OTLP receiver and DogStatsD to its `:8125`.
- **Standing in for an Agent.** Applications point their Datadog libraries at `logit` instead of an
  Agent: [`examples/datadog-agent-standin.yaml`](../examples/datadog-agent-standin.yaml) listens
  for DogStatsD on `:8125` (`statsd_in`) and for traces and client stats on `:8126`
  (`datadog_trace_in`), optionally on the Agent's Unix sockets too.
  [`examples/datadog-agent-relay.yaml`](../examples/datadog-agent-relay.yaml) relays both on to a
  real Agent, which is where tracer spans must go next (see
  [Rules that lose data when missed](#rules-that-lose-data-when-missed)).
- **Standing in for the intake.** Agents send `logit` what they'd send Datadog, over Datadog's
  intake API: [`examples/datadog-intake-standin.yaml`](../examples/datadog-intake-standin.yaml)
  runs `datadog_in` and has the Agent-side settings for replacing Datadog and for dual-shipping.
  This is the topology a migration away from Datadog runs through
  ([From Datadog](#from-datadog-dual-ship-tee-then-cut-over)).

Which component carries each signal in each topology:

| Signal | Direct | Through a local Agent | Agent stand-in | Intake stand-in |
|---|---|---|---|---|
| Metrics | `datadog_out` (series, sketches, distribution points); `otlp_out` agentless, delta temporality only | `statsd_out format: dogstatsd`, over UDP or either Agent Unix socket; `otlp_out` | `statsd_in`, over UDP or either Agent Unix socket; `otlp_in` | `datadog_in` |
| Logs | `datadog_out`; `otlp_out` agentless | `otlp_out`, with the Agent's OTLP logs turned on | `syslog_in`; `otlp_in` | `datadog_in` |
| Traces | `datadog_out` for Agent-processed spans with their APM stats; `otlp_out` agentless for OTel spans | `otlp_out` for OTel spans; `datadog_trace_out` for tracer spans | `datadog_trace_in`; `otlp_in` | `datadog_in` (spans and APM stats) |
| Events | `datadog_out` | `statsd_out` (`_e{}`) | `statsd_in` (`_e{}`) | `datadog_in` |
| Service checks | `datadog_out` | `statsd_out` (`_sc`) | `statsd_in` (`_sc`) | `datadog_in` |

Two log paths aren't covered. An application that writes JSON lines to an Agent's TCP `logs`
listener has no plain-lines listener to switch to in `logit`. And an Agent's TCP `logs` listener
takes syslog-formatted lines from `syslog_out`, but whether the Agent parses a JSON body in one
into attributes is untested. Use `otlp_out` for logs through an Agent.

## Which way to send

### To Datadog: direct or through an Agent

**Send directly when `logit` is already the collector on the host**: `datadog_out` for metrics,
logs, events, service checks, and Datadog-origin traces with their APM stats, and `otlp_out`
agentless for OTel-origin traces. **Put an Agent in front when you want Datadog's host and APM
features**, or when OTel-origin traces need the Agent's sampling and ingestion controls. Whichever
you pick, **don't send one signal both ways**: Datadog stores both copies, so the same data
arrives twice.

A local Agent gives you:

- host and container metadata, Live Containers, Datadog's integrations, and the Agent's
  out-of-the-box tags;
- APM trace metrics and Remote Configuration;
- its own retry and buffering;
- Unix-socket locality for DogStatsD and APM;
- Agent-side aggregation of `h` and `ms` into `.avg`/`.count`/`.median`/`.95percentile`/`.max`,
  and of `d` into sketches.

It costs you:

- one more process on every host;
- OTLP logs, which are off in the Agent by default;
- HTTP log intake, which the Agent doesn't have: logs reach it as TCP lines or OTLP only;
- a Datadog dependency on every host for the whole of a migration.

Sending directly needs no Agent: it's one hop, and it works wherever HTTPS does. It costs you:

- the 1-hour metric window, which drops a disk-buffer replay after a long outage
  ([below](#disk-buffer-replay-meets-the-1-hour-metric-window));
- the Agent's `h`/`ms` aggregates, which nothing in `logit` produces. `aggregate` sends timers and
  histograms as sketches, and Datadog computes quantiles from those;
- distributions go only as raw points (`/api/v1/distribution_points`) or through the sketches
  route, which Datadog doesn't document for third parties but accepts;
- the intake's payload limits, which `datadog_out` splits requests under;
- for traces, a choice of protocol. The native protocol carries Datadog-origin spans losslessly
  with the stats their Agent computed. OTLP carries OTel-origin spans, but Datadog computes
  their trace metrics (`compute_stats=true`) from the spans that arrive, so after any sampling,
  and Datadog's OTLP mapping loses parts of a Datadog span
  ([the plan's §12](plans/datadog-relay.md#12-traces-to-datadog-the-agents-protocol-not-otlp-for-datadog-origin-spans-w5-w7)).

### From Datadog: dual-ship, tee, then cut over

A migration away from Datadog runs through the intake stand-in in three stages:

1. **Dual-ship.** Add `logit`'s `datadog_in` to the Agents' `additional_endpoints`, and to the
   `logs_config` and `apm_config` equivalents. Datadog keeps receiving everything, and no
   application or host changes. Undoing it is one Agent config edit.
2. **Tee.** Fan `datadog_in` out to the new backend while Datadog keeps receiving its own copy,
   and compare the two.
3. **Cut over.** Point `dd_url`, `logs_config.logs_dd_url`, and `apm_config.apm_dd_url` at
   `logit`, or remove the Agent.

Use the Agent stand-in (`statsd_in`, `datadog_trace_in`, and `otlp_in` on the Agent's ports) only
where there's no Agent to redirect, such as containers without a sidecar or serverless functions,
or in stage 3, once the Agent itself is going away.

## Rules that lose data when missed

### `datadog_trace_in` must not feed `datadog_out`

Datadog's trace intake expects spans an Agent has normalized, obfuscated, and marked, with the
Agent's APM stats beside them. `datadog_trace_in` does none of that processing: it relays spans as
the tracer wrote them. `datadog_out` sends a trace chunk only when its root span carries the
Agent's `_top_level` mark, so every span from `datadog_trace_in` is dropped and counted
`logit.output.records.dropped{reason="needs_agent_processing"}`. An OTel span with no Datadog
fields at all is counted `reason="not_datadog_origin"`.

Send `datadog_trace_in`'s output to `datadog_trace_out` in front of a real Agent, as
`datadog-agent-relay.yaml` does, or to an OTLP backend. Send OTel spans to Datadog with
`otlp_out`. What `datadog_in` receives on `/api/v0.2/traces` came from an Agent, so `datadog_in`
into `datadog_out` relays traces unchanged.

### Disk-buffer replay meets the 1-hour metric window

`datadog_out` drops data outside Datadog's documented windows before sending, counted
`logit.output.records.dropped{reason="stale"}`: metrics older than 1 hour or more than 10 minutes
ahead, logs and events older than 18 hours, and service checks older than 10 minutes. A
`buffer.disk:` holds batches through a Datadog outage, but on replay after an outage longer than
an hour, the metrics are dropped as stale, not delivered late. The 1-hour window is the documented
one and stricter than what the intake stored in testing
([the plan's "Timestamp windows"](plans/datadog-relay.md#11-timestamp-windows-w5)).

### `at_least_once` duplicates everything but series

`datadog_out` isn't duplicate-safe. One batch goes out over up to eight routes, each as one or
more requests (one per event, and a route over its size cap is split), and a retry re-sends the
ones that succeeded. Datadog stores a resent series point once, the last write winning at its
`(series, timestamp)`, but stores a resent log twice. Every other route is assumed to duplicate
too. So the default posture is at-most-once, and a `5xx` or a timeout drops the batch.
`buffer: {delivery: at_least_once}` retries instead and accepts duplicate logs, events, and
checks, and inflated distribution, sketch, trace, and stats counts. `datadog_trace_out` is the
same: an Agent dedupes nothing.

### Events go uncompressed

Datadog's `/api/v1/events` answers any gzip or deflate body `400 Invalid JSON structure`, so
`datadog_out` sends events uncompressed whatever `compression:` says. Every other route follows
`compression:` (gzip by default; zlib deflate for distribution points).

### Size limits

`datadog_out` cuts each route's events into requests under Datadog's limits, and an event too
large to send alone is dropped and counted `records.dropped{reason="oversize"}`. The series limit
(512,000 bytes compressed, 10,000 points) is the intake's own. The limits on distribution points,
sketches, and logs are tighter than the intake's, so they cost extra requests but never a `413`.
`datadog_in` caps a request at 5 MiB compressed and 5,242,880 bytes decompressed (16 MiB for
traces), above what a conforming Agent sends. Both components' `deploying.md` sections have the
tables.

### A `503` is deferral for an Agent, and loss for a tracer

When the pipeline doesn't take a request's batches in time, both listeners answer `503` with
`Retry-After: 1` rather than holding the connection, and count
`logit.input.batches.dropped{reason="busy"}`. What that costs depends on the sender:

- **`datadog_in`** waits 5 seconds. An Agent's forwarder keeps the payload in its retry queue and
  retries for minutes, so a `503` defers delivery until that queue fills.
- **`datadog_trace_in`** waits 2 seconds. dd-trace-py 4.15.2 retries a `503` five times, waiting
  100 ms and doubling, then drops the payload. Against this listener that's a window of about
  15 seconds; a longer stall loses spans.

Give the sinks behind `datadog_trace_in` a `buffer:` (memory, or `disk:` for a long outage) large
enough to absorb a stall, so the channel it sends into keeps draining. A `buffer:` on
`datadog_trace_out` also keeps an unreachable Agent from turning into `503`s at the tracers.

### Unix sockets: create the directory, mind the mode

`statsd_in` (`transport: unix` or `unix_stream`) and `datadog_trace_in` (`socket:`) bind the
Agent's socket paths. `logit` never creates the socket's directory, because its owner and mode are
the access control. It makes the socket file mode `0722`, the Agent's own mode: a client needs
only write permission to send, so any user's process can. Restrict senders with the directory's
permissions. The path must be absolute, and neither Unix transport takes `tls:`. On the sending
side, raise `statsd_out`'s `max_packet_bytes:` to `8192` for a Unix socket.

### Log correlation: `trace_context` with `format: datadog`

A dd-trace tracer with log injection on writes the active trace and span into each log line.
`trace_context`'s default `otel` format reads only W3C hex ids, so those lines stay uncorrelated
until you set `format: datadog`, which reads `dd.trace_id` and `dd.span_id`:

- **dd-trace-py** (4.15.2, in the trial-org run) writes flat dotted keys: `dd.trace_id` as 32 hex
  characters when the tracer generates 128-bit ids, else decimal, and `dd.span_id` as decimal,
  beside `dd.service`, `dd.env`, and `dd.version`.
- **dd-trace-js** with pino or winston, and **dd-trace-rb** with lograge, nest the same fields
  under a `dd` object (`"dd":{"trace_id":"...","span_id":"..."}`). Put a `flatten` with
  `attributes: [dd]` ahead of `trace_context` to turn that into the dotted names. It's a no-op on
  a line with no `dd` object. No real output from these two libraries has been checked.

Three details catch people out:

- **A line that carries both forms loses one silently.** `flatten` writes `dd.trace_id` over a
  flat `dd.trace_id` already on the event, last write winning, with no counter.
- **A 16-digit id is decimal under `datadog`.** The format decides; nothing is guessed from the
  value. A decimal trace id fills the low 64 bits; name an attribute holding `_dd.p.tid`'s high
  half in `trace_id_high:` if your logs carry it.
- **`datadog_out` sends the log body as the Datadog `message`**, and drops an attribute named
  `message` or `timestamp`, counted `logit.output.tags.dropped{reason="reserved_key"}`. After
  `json`, the body is still the original line. The lifted ids go out as hex `trace_id` and
  `span_id`, which Datadog's log intake correlates on, unless the log already has an attribute of
  either name.

[`examples/datadog-logs-correlation.yaml`](../examples/datadog-logs-correlation.yaml) runs
`tail_in`, `json`, `flatten`, and `trace_context` into both `datadog_out` and `otlp_out`.

### Credentials

Take `datadog_out`'s `api_key` and `datadog_in`'s `api_keys` from the environment with
`!env DD_API_KEY`, as every example does. `datadog_out` rejects a key with leading or trailing
whitespace at startup, since a key file read with its trailing newline would reach Datadog as a
different key, and it never logs the key. `datadog_in` with `api_keys` empty accepts any key,
including on the Agent's key check, so an Agent can't detect a mistyped key; set `api_keys`. It's
a shared secret, not transport security: add `tls:` before binding beyond loopback.

### Sites

`datadog_out`'s `site` defaults to `datadoghq.com`. Set it to your organization's site:
`datadoghq.eu`, `us3.datadoghq.com`, `us5.datadoghq.com`, `ap1.datadoghq.com`,
`ap2.datadoghq.com`, `uk1.datadoghq.com`, `ddog-gov.com`, or `us2.ddog-gov.com`. A key from one
site gets `403` on another. For `otlp_out` agentless, the endpoint is `https://otlp.<site>` with a
`dd-api-key` header, over HTTP only; Datadog's OTLP intake takes delta metrics, not cumulative.

## What's verified

The codecs and listeners were checked against real traffic recorded from Datadog Agent 7.83.3,
dd-trace-py 4.15.2 (libdatadog 9.0.0) with Flask 3.1.3, and the `datadog` DogStatsD client 0.54.0
over both Unix sockets ([`testdata/interop/datadog/`](../testdata/interop/datadog/README.md)).
Every recorded request decodes and gets its route's `2xx` from the listeners, and a recorded
series request and the recorded tracer requests relay through the two Datadog pairs unchanged.

A Datadog trial org then received, and showed, what `logit` sent it:

- `datadog_out` direct: series, sketches, distribution points, logs, and events, with the stale
  window, the series dedupe, and the size caps above measured against the intake. Service checks
  got `202`, but Datadog has no API to query them back.
- A real Agent pointed at `datadog_in` with `dd_url`, `logs_config.logs_dd_url`, and
  `apm_config.apm_dd_url`, relayed by `datadog_out`: its metrics, logs, and events arrived; its
  spans arrived with their 128-bit trace ids and resource names; and its relayed APM stats
  populated the service's `trace.*` hits, errors, and latency.
- `datadog_trace_out` and `statsd_out` into a real Agent over TCP, UDP, and its Unix
  sockets, with the tracer's request headers accepted.
- A real dd-trace-py log line lifted by `trace_context` under `format: datadog`, found by its
  trace id in Datadog's log search.
- `otlp_out` agentless: a span, a delta sum, and a log.

The Agent's dual-shipping (`additional_endpoints`) and TLS settings against `datadog_in` are
untested. `docs/known-gaps.md`'s "Datadog" section lists everything else that isn't built or
isn't verified.
