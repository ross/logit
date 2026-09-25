---
created: 2026-09-24
updated: 2026-09-24
---

# Enabling plan: SigNoz — OTLP-native, verified, no new kind

## Context

`logit` has no SigNoz-specific component and needs none. SigNoz ingests OpenTelemetry Protocol
(OTLP) and nothing else of its own that OTLP doesn't already cover, so `otlp_out` with a
`headers:` block reaches it today, unverified. This plan records what SigNoz accepts, where the
event model and `otlp_out`'s encoder fall short of it, and the workstreams that verify the leg
against a real instance and ship an example.

Goals:

- Send logs, metrics, and traces to a self-hosted SigNoz over `otlp_out`, verified against a
  pinned release, with an example config and an operator doc.
- Stand in front of SigNoz as the multi-protocol edge its default collector config doesn't
  offer: statsd, syslog, carbon, collectd, Prometheus, and file tails in, one OTLP leg out.
- Record every mapping SigNoz applies to what `otlp_out` sends, so an operator knows what their
  SigNoz UI shows for each `logit` metric kind before they configure it.

Non-goals: SigNoz's HTTP JSON logs API (`httplogreceiver`), which carries nothing OTLP logs
don't; SigNoz Cloud verification, which needs an account (its endpoint and header are recorded
below, UNVERIFIED); a SigNoz query-API input, since SigNoz is a store and emits nothing on its own;
SigNoz's UI-managed logs pipelines, which run inside its collector and compete with `logit`'s
transforms rather than replace them.

Stream key **`signoz`**: branches `signoz/w0`…`signoz/w2`, linear. PR stack only: nothing is
merged by this workstream; Ross directs merging. The decisions are recorded in ADR
`signoz-over-otlp` (W1), written after the W1 verification rather than before it, because the
facts it records are about SigNoz's behavior on receipt, not about a documented wire.

Settled with Ross (2026-09-24): verify first, no new components, self-hosted only.

## Coverage by signal

SigNoz is one product with one store (ClickHouse) for all three signals. Each cell reads
today → after this stack.

| Signal | Self-hosted, direct | SigNoz Cloud | Receive from SigNoz |
|---|---|---|---|
| Logs | `otlp_out` OTLP/HTTP or OTLP/gRPC to the bundled collector (unverified) → verified (W1), with an example (W2) | `otlp_out` with `signoz-ingestion-key` (UNVERIFIED, out of scope) | none: SigNoz is a store, it emits nothing |
| Metrics | same → verified per metric kind (W1) | same | none |
| Traces | same → verified, including `http_access`/`trace_context` spans in the APM views (W1) | same | none |

## What SigNoz accepts

Surveyed 2026-09-24 from `signoz.io/docs`, `github.com/SigNoz/signoz`, and
`github.com/SigNoz/signoz-otel-collector`. Items marked UNVERIFIED were not confirmed by a
current official page; W1 verifies each against the pinned self-hosted release and this section is
updated then.

### OTLP: the one door

SigNoz's ingest is an OpenTelemetry Collector distribution (`signoz-otel-collector`) writing to
ClickHouse (`signoz_traces`, `signoz_metrics`, `signoz_logs`). Self-hosted, its reference config
wires two receivers into pipelines: `otlp` (gRPC `:4317`, HTTP `:4318`, protobuf and JSON) for all
three signals, and a `prometheus` receiver that only scrapes the collector's own internal metrics.
The binary compiles in the whole `opentelemetry-collector-contrib` receiver set (`statsd`,
`syslog`, `carbon`, `collectd`, `fluentforward`, `jaeger`, `zipkin`, `prometheusremotewrite`,
`splunk_hec`, `datadog`, and more), none of them enabled. An operator who wants those protocols
edits the collector YAML, or puts `logit` in front.

SigNoz's own components, all collector-side and none of them a wire format: receivers
`httplogreceiver`, `clickhousesystemtablesreceiver`, `signozkafkareceiver`,
`signozawsfirehosereceiver`; processors `signozspanmetricsprocessor`, `signoztailsampler`,
`signoztransformprocessor`, `signozlogspipelineprocessor`, `signozspanmapperprocessor`; exporters
`clickhouselogsexporter`, `clickhousetracesexporter`, `signozclickhousemetrics`, `metadataexporter`.
Everything else is stock contrib.

| Target | Endpoint | Auth and transport |
|---|---|---|
| Self-hosted | `http://<collector>:4318` (OTLP/HTTP, `/v1/{logs,metrics,traces}`), `<collector>:4317` (OTLP/gRPC) | none by default; TLS and auth are the operator's ingress |
| SigNoz Cloud | `ingest.<region>.signoz.cloud:443`, one host for both OTLP/HTTP and OTLP/gRPC | `signoz-ingestion-key: <key>` header (basic `Authorization` also accepted; `signoz-access-token` is a legacy name, UNVERIFIED as still accepted); TLS always; gzip accepted |
| Self-hosted, JSON logs | `http://<collector>:8082` (`httplogreceiver`, `source: json`); Cloud `/logs/json` | a JSON array of log objects "similar to" the OTel log model; not a target of this plan |

Pinned for W1: `signoz/signoz` v0.143.0 and `signoz/signoz-otel-collector` v0.144.11, the
latest releases on the survey date. Schema migrations run from the collector image (since
v0.113); the standalone `signoz/signoz-schema-migrator` image is superseded.

### What the UI reads

- **Traces.** The service map and APM views read OTel semantic conventions: `service.name`
  (required for a span to appear under a service), span kind, `db.system`, `net.peer.name`,
  `messaging.*`, `rpc.*`. For HTTP, the UI reads the pre-1.23 names `http.method`, `http.url`,
  `http.status_code`; SigNoz tracks moving to `http.request.method`, `url.full`, and
  `http.response.status_code` in `https://github.com/SigNoz/signoz/issues/8406`. Which set the
  pinned release resolves, and whether it reads both, is UNVERIFIED.
- **Metrics.** Gauge, monotonic Sum, Histogram, and ExponentialHistogram are stored. Cumulative
  and delta temporality are accepted for Sum and Histogram; ExponentialHistogram is documented as
  delta-only. Series identity is metric name, temporality, resource attributes, scope, and point
  attributes. Whether `Summary` is stored, and how, is UNVERIFIED. Whether metric names are
  normalized (dots to underscores) at OTLP ingest is UNVERIFIED.
- **Logs.** `body`, `severity_text`/`severity_number`, `trace_id`/`span_id`, attributes, and
  resource attributes are the indexed fields. What a `Map` body, `event_name`, and
  `observed_timestamp` become in the logs explorer is UNVERIFIED.

### Reading back

A service-account API key authorizes `POST /api/v5/query_range` (traces, logs, and metrics,
including PromQL and ClickHouse SQL). W1 uses it to confirm receipt of each signal without
scraping the UI.

### Unverified, to be settled by W1

1. Which HTTP semconv names the pinned release's APM views read: the pre-1.23 set, the current
   set, or both.
2. Whether OTLP `Summary` is stored, and what the explorer shows for one.
3. Whether a `Map` log body survives as a structured body, is flattened to attributes, or is
   stringified; and what `event_name` and `observed_timestamp` become.
4. Whether a cumulative ExponentialHistogram is rejected, silently dropped, or stored.
5. Whether metric names are normalized at OTLP ingest.
6. Which Cloud auth header names the current ingest accepts.

## SigNoz's data against `Event`

SigNoz consumes OTLP as-is, so the fit is `otlp_out`'s fit, already recorded in
[`known-gaps.md`](../known-gaps.md)'s cross-protocol table. This table restates the rows a SigNoz
operator hits and adds what SigNoz does on its side.

| `Event` kind | `otlp_out` today | SigNoz | Verdict |
|---|---|---|---|
| `LogRecord` with a `Str` body, severity, `TraceRef`, attributes | exact | indexed as documented | lossless |
| `LogRecord` with a `Map`/`Array` body (`json`'s output), `event_name`, `observed_timestamp` | exact | UNVERIFIED | lossless on the wire; W1 settles what the explorer shows |
| `Sum`, `Gauge`, `Histogram` | exact, both temporalities | stored | lossless |
| `ExponentialHistogram` | exact | delta stored; cumulative UNVERIFIED | lossless under delta; W1 settles the cumulative case |
| `Summary` | exact, exemplars dropped | UNVERIFIED | W1 settles |
| `Samples`, `Distribution` | degraded to a `Summary` of five fixed quantiles, counted `logit.output.metrics.degraded` | as `Summary` | degraded at `otlp_out`, then whatever item 2 finds; this is the row that decides item 3 of the design |
| `Set`, `SetMembers`, `GaugeDelta` | skipped, counted `logit.output.metrics.skipped` | never arrive | a known OTLP gap, not a SigNoz one |
| `SpanRecord` with semconv attributes | exact | APM views read semconv; HTTP names per item 1 | lossless on the wire; the UI's name set decides item 2 of the design |
| batch `Resource` and `Scope` | exact | resource attributes become series and log dimensions; `service.name` is the service | lossless; set upstream with `set`, per [`operator-declared-resource-attributes`](../adr/operator-declared-resource-attributes.md) |

`aggregate` has no mode that turns a `Samples` series into a `Histogram`: `distributions: sketch |
samples` chooses between a `DDSketch` and raw values, and both reach `otlp_out` as a `Summary`. If
W1 finds SigNoz drops or misreads `Summary`, a statsd timer has no working path into SigNoz's
histogram views, and the follow-up is an `aggregate` option that emits explicit-bucket
`Histogram`s from operator-declared bounds. That's a core change outside this stack, recorded in
`known-gaps.md` by W1 if it's needed.

## Direct, or through SigNoz's collector

There is no third option. `otlp_out` → `signoz-otel-collector` is already "through" SigNoz's
collector: its span-metrics, tail-sampling, and logs-pipeline processors run in front of
ClickHouse regardless of what sent the OTLP. The choice is what stands in front of it.

**`logit` in front.** For: one process on the host speaks every protocol `logit` speaks, losslessly,
and the collector YAML stays stock; `aggregate` and the parsers run once, with `keep`/`keep_values`
bounding cardinality before it reaches ClickHouse. Against: a second hop for OTLP-native producers,
which can point at SigNoz directly.

**SigNoz's collector alone.** For: no second process; SigNoz's supported path. Against: every
non-OTLP protocol means editing the collector config to enable a contrib receiver and restarting
it, and those receivers carry contrib's own lossy mappings, not `logit`'s.

**Best practice.** Point OTLP-native SDKs at SigNoz directly; put `logit` in front for everything
else. Parse a field once: a `logit` `json`/`regex` stage and a SigNoz logs pipeline on the same
field is two parsers disagreeing. Set `service.name` with `set` at the edge; a span without it is
invisible in the APM views.

## Design

Each item is a decision ADR `signoz-over-otlp` records, and names the workstream that settles
or ships it.

### 1. No SigNoz kind (W1)

`otlp_out` plus `set` for `service.name` is the whole configuration. A `signoz_out` would be
`otlp_out` with a fixed header name, which `headers:` already expresses (with `!env` for the key),
and the JSON logs API duplicates OTLP logs with less structure. ADR
[`otlp-tls-and-pooled-grpc-client`](../adr/otlp-tls-and-pooled-grpc-client.md) covers the
transport, TLS, and gzip side; nothing there changes.

### 2. HTTP semconv names (W1 decides, W2 ships)

`http_access` emits current semconv (`http.request.method`, `http.response.status_code`,
`url.*`) and stays that way; that's its contract under
[`http-access-normalization`](../adr/http-access-normalization.md). If W1 finds the pinned SigNoz
reads only the pre-1.23 names, the example carries a stage that copies the current names onto
the old ones (`set` can't derive one attribute from another, so it's a short `lua` component),
labeled as the shim it is and removable once SigNoz's issue closes. It is not an `http_access`
option: an output-side vocabulary quirk doesn't belong in the normalizer.

### 3. Timer and distribution metrics (W1 decides, W2 ships)

The example sends statsd timers through `aggregate` as it is, so they reach SigNoz as `Summary`
points, and the example's comment states that. If W1's item 2 finds `Summary` unusable in SigNoz,
the example says so and `known-gaps.md` gains the row described under "SigNoz's data against
`Event`"; the `aggregate` option is a follow-up, not part of this stack.

### 4. Cloud, documented but unverified (W2)

The example carries a commented-out Cloud variant: `endpoint: https://ingest.<region>.signoz.cloud`,
`headers: { signoz-ingestion-key: !env SIGNOZ_INGESTION_KEY }`, `compression: gzip`, marked
UNVERIFIED. Verifying it is a one-line change to W1's checklist for whoever has an account.

### 5. The verification harness is throwaway (W1)

W1 runs SigNoz from a compose file under `tmp/`, not committed: `signoz/signoz`,
`signoz/signoz-otel-collector`, and ClickHouse at the pinned tags, with a `logit` container on the
same network. Nothing SigNoz emits is a fixture (it emits nothing), so `script/record-fixtures`
and `testdata/interop/` are untouched. The findings land in this plan, the ADR, and the example's
comments. A committed compose file next to `fixtures/nginx/` is a possible W2 addition if the
findings make one worth keeping; the default is not to ship one.

## Workstreams

| # | PR | Size | Depends on |
|---|---|---|---|
| W0 | This plan and its index row | S | — |
| W1 | Verify against self-hosted SigNoz at the pinned tags: `otlp_out` over HTTP and gRPC with logs (`Str` and `Map` bodies, `event_name`, `observed_timestamp`), every metric kind `otlp_out` emits (a `Summary`, both `Histogram` temporalities, both `ExponentialHistogram` temporalities, a degraded `Samples`), and spans from `otlp_in`, `trace_context`, and `http_access`; read back through `/api/v5/query_range` and the UI; resolve every UNVERIFIED item and update this plan; ADR `signoz-over-otlp` and its `docs/adr/README.md` row; a `known-gaps.md` row if item 2 or 4 needs one | S | W0 |
| W2 | `fixtures/signoz.yaml` (`statsd_in`, `syslog_in`, `otlp_in` → `set`, `json`, `aggregate` → `otlp_out` over gRPC to `signoz-otel-collector:4317`, with the shim from design item 2 if W1 called for it and the Cloud variant commented out); a "SigNoz" subsection in `docs/deploying.md`; `AGENTS.md`'s examples list; `SIGNOZ_INGESTION_KEY` in `every_shipped_config_loads_and_validates`'s `!env` map (`crates/logit-cli/src/config.rs`) if the example resolves it | S | W1 |

Landing order: W0 → W1 → W2, linear. Each PR is based on and targets its parent's branch and is
brought up to date with `git merge origin/main`, never a rebase.

**Status (2026-09-24):** W0 open.

## Verification

- Per PR: `script/cibuild` green; `script/validate` for any PR that adds a config.
- W1: each signal sent by `otlp_out` is returned by `/api/v5/query_range` and visible in the
  explorer; a span from `http_access` appears under its `service.name` in the APM views with its
  method and status; every UNVERIFIED item in this plan is resolved and the text updated;
  `type_sizes.rs` and `allocations.rs` unchanged (no model change).
- W2: `fixtures/signoz.yaml` passes `logit validate` and, against the W1 harness, shows a statsd
  counter, a syslog line, and an OTLP span in SigNoz.
- W0 (this PR) is documentation only: every relative link resolves and `docs/plans/README.md`
  gained a row.
