---
created: 2026-10-24
updated: 2026-10-24
---

# Enabling plan: the remaining common keys — service, deployment, container, and process identity

## Context

Every input with a protocol host field now writes `host.name`, and no sink rebuilds a host from a
wire form. The same facts for service, deployment environment, version, container, and process
reached the model under protocol spellings: a bare `env` tag, a `_logit.datadog.tracer.env`
carrier, a `_logit.statsd.container_id` carrier, a string-or-integer `syslog.pid`. A sink that wanted
one of them had to know every family's spelling, and a cross-family relay (`datadog_in` to
`otlp_out`, say) delivered none of them under the name its destination reads.

This plan moves each of those facts onto its OpenTelemetry semantic-conventions key, input by
input, under stream key `ckeys`. Every workstream below has landed.

Goals:

- Every input whose protocol defines one of the six keys in
  [ADR `process-hostname`](../adr/process-hostname.md#amendment-the-remaining-common-keys-2026-10-24)
  writes it, at
  the scope where it is identity, and the carrier or bare attribute it replaces is gone.
- Every sink in that input's family writes the key back in its own wire form, so each like-pair
  relay stays a fixed point modulo its named normalizations.
- A Datadog-family sink writes each key under Datadog's spelling whatever source wrote it, so
  an OTLP sender's `deployment.environment.name` reaches Datadog as `env`.

Non-goals:

- Any key an input would have to infer from a field its protocol doesn't define. A Telegraf
  `env` tag on `influxdb_in`, a parsed log's `service` field on `tail_in`, or a carbon path
  segment stays what it is, and an operator's transform puts it on a common key.
- The fields the same amendment lists under "What stays as it is".
- `otlp_out` and `splunk_hec_out` changes beyond what a test shows is needed. Both pass the keys
  through as attributes.

## The rule

A source writes the common keys from its incoming data, with its protocol's own logic, at the
scope where the value is identity; a sink uses them and never rebuilds one from a wire form; a
wire form that names the same fact decodes onto the common key and doesn't survive as a carrier.
An input writes a key only from a field its protocol defines, and where the wire form's meaning is
ambiguous, the default is to write nothing, with a config option. The rule and its reasons are
[ADR `process-hostname`](../adr/process-hostname.md)'s "Amendment: a source fills the common keys
(2026-10-23)"; this stream's decisions are that ADR's "Amendment: the remaining common keys
(2026-10-24)".

## Settled calls

The decisions are recorded once, in ADR `process-hostname`: its "Amendment: the remaining common
keys (2026-10-24)" has the six keys and their types, the scope rule, the two collision rules,
Datadog's spellings at a Datadog-family sink, and what stays as it is, with a reason for each, and
the amendments after it have each family's forms, among them the `ddtags` lift and why a full
split was rejected, and DogStatsD `|c:`. This plan adds the code each decision landed in.

Code the cross-cutting rules follow:

- **Scope on encode.** A per-payload field follows the Datadog encoder's `TracerPayload`
  `hostname` rule, in `is_carrier` and `DatadogEncoder::wire_span` in
  `crates/logit-proto/src/datadog/traces.rs`, which `datadog_out` and `datadog_trace_out` share.
  The form with no payload field is `datadog_trace_out`'s v0.4.
- **Collision at decode.** `logit.input.tags.omitted{reason="reserved_key"}` is what
  `insert_host` in `crates/logit-proto/src/statsd/decode.rs` and `insert_wire_host` in
  `crates/logit-proto/src/datadog/mod.rs` count for a `host.name:` tag beside a line's own host.
- **Collision at encode.** `statsd_out` counts a valued `host` beside `host.name` as
  `logit.output.tags.omitted{reason="unrepresentable"}`; the Datadog log encoder in
  `crates/logit-proto/src/datadog/logs.rs` counts a log's `hostname` attribute as
  `reason="reserved_key"`.

## Per-input mapping

Each row is what the decoder wrote before the stream, read from the code. "Before" names the
attribute and its scope; "Becomes" is the key it moved to, on the same scope unless the row says
otherwise.

### Container identity (W1)

| Input | Wire field | Before | Becomes | Code |
|---|---|---|---|---|
| `statsd_in` | `\|c:` on a metric line, event, or service check | `_logit.statsd.container_id`, event, the raw field | `container.id`, event | `insert_container_id` in `crates/logit-proto/src/statsd/decode.rs` |
| `datadog_trace_in` | `Datadog-Container-ID` header | `_logit.datadog.tracer.container_id`, resource, filled only when the payload's own field is empty | `container.id`, resource | `apply_tracer_headers` (through `TRACER_STR_HEADERS` in `crates/logit-proto/src/datadog/mod.rs`) and `apply_stats_headers` (through `STATS_HEADERS`) in `crates/logit-inputs/src/datadog_trace.rs` |
| `datadog_in` traces, `datadog_trace_in` `/v0.7/traces` | `TracerPayload.container_id` | `_logit.datadog.tracer.container_id`, resource | `container.id`, resource | `tracer_resource` in `crates/logit-proto/src/datadog/traces.rs` |
| `datadog_in` stats, `datadog_trace_in` `/v0.6/stats` | `ClientStatsPayload.ContainerID` | `_logit.datadog.tracer.container_id`, resource (the tracer carrier, shared with traces) | `container.id`, resource | `PAYLOAD_STRINGS` and `payload_string_mut` in `crates/logit-proto/src/datadog/stats.rs` |
| `datadog_in` stats, `datadog_trace_in` `/v0.6/stats` | `ClientStatsPayload.ImageTag` | `_logit.datadog.stats.image_tag`, resource | `container.image.tags`, a one-element array, resource | the same |
| `docker_in` | the image reference's tag in `config.v2.json` | `container.image.tag`, resource | `container.image.tags`, a one-element array, resource | `resource` in `crates/logit-inputs/src/docker.rs` |

`datadog_out` wrote no container identity on its metrics, events, checks, or logs routes before
W1, so W1's `container_id:` and `image_tag:` tags are new output there. Its traces and stats
routes wrote `container_id`, `ContainerID`, and `ImageTag` from the carriers (`wire_tracer` in
`traces.rs`, `client_payload` in `stats.rs`), and W1 moved those reads to `container.id` and
`container.image.tags`. `statsd_out` wrote `|c:` from the carrier.

**`|c:` isn't always a container id.** The Agent reads it as origin-detection local data, and
the decoder kept the whole field verbatim. ADR `process-hostname`'s "Amendment: container identity
(2026-10-24)" and ADR `statsd-output`'s "Amendment (2026-10-24): `|c:` is the Agent's local data"
say how each form decodes and leaves.

### Process identity (W2)

| Input | Wire field | Before | Becomes | Code |
|---|---|---|---|---|
| `syslog_in` | RFC 5424 PROCID | `syslog.pid`, event: `U64` when canonical decimal, else `Str` | `process.pid` (`U64`) when canonical decimal | `parse_5424` and `canonical_pid` in `crates/logit-proto/src/syslog/decode.rs` |
| `syslog_in` | RFC 3164 `tag[pid]` | the same | the same | `parse_3164` |

Canonical decimal is ASCII digits with no leading zero, or `0`. A PROCID that isn't (`-` aside,
which writes nothing) stays the string `syslog.pid`, since it isn't a process id and `syslog_out`
has to relay it unchanged. `syslog_out` reads both back as PROCID (`resolve_pid` in
`crates/logit-outputs/src/syslog.rs`, which read `syslog.pid` alone before W2).

### Service identity on tags (W4a)

| Input | Wire field | Before | Becomes | Code |
|---|---|---|---|---|
| `statsd_in` | `env:`, `version:`, `service:` tags on a metric line, event, or service check | bare `env`, `version`, `service`, event | the three keys, event | `insert_tags` in `crates/logit-proto/src/statsd/decode.rs` |
| `datadog_in` | the same tags on a series (v1, v2 JSON, v2 protobuf), distribution point, or sketch | the same | the same | `insert_tags` in `crates/logit-proto/src/datadog/tags.rs`, called from `series.rs` |
| `datadog_in` | the same tags on an event or service check | the same | the same | the same, called from `events.rs` and `service_checks.rs` |

### Service identity on Datadog logs (W4b)

| Input | Wire field | Before | Becomes | Code |
|---|---|---|---|---|
| `datadog_in` logs | `service` | `service`, event | `service.name`, event | `ATTR_SERVICE` in `crates/logit-proto/src/datadog/logs.rs` |
| `datadog_in` logs | `ddtags` | one `Str` `ddtags`, event, never split | `env:`, `version:`, and `service:` lifted onto the keys; the rest stays `ddtags` | `ATTR_DDTAGS`, the same file |

`datadog_out` wrote a log's `service` from `service`, else from `service.name`, before W4b
(`LogKeys` and the encoder's reserved-field loop in `logs.rs`).

### Service identity on traces and stats (W4c)

| Input | Wire field | Before | Becomes | Code |
|---|---|---|---|---|
| `datadog_in` traces, `datadog_trace_in` | a span's `env` and `version` meta | bare `env`, `version`, event (every `meta` key lands verbatim) | `deployment.environment.name`, `service.version`, event | `span_event` in `crates/logit-proto/src/datadog/traces.rs` |
| the same | `TracerPayload.env`, `.app_version` | `_logit.datadog.tracer.env`, `.app_version`, resource | `deployment.environment.name`, `service.version`, resource | `tracer_resource` in `traces.rs` |
| `datadog_in` stats, `datadog_trace_in` `/v0.6/stats` | `ClientStatsPayload.Env`, `.Version` | `_logit.datadog.tracer.env`, `.app_version`, resource (the tracer carriers) | the same two keys, resource | `payload_string_mut` in `stats.rs` |
| the same | `ClientStatsPayload.Service` | `_logit.datadog.stats.service`, resource | `service.name`, resource | the same |

A span's `service` and a stats group's `Service` were `service.name` on the event before the
stream (`ATTR_SERVICE_NAME` in `crates/logit-proto/src/datadog/mod.rs`).

## Workstreams

| WS | Branch | Scope | Status |
|---|---|---|---|
| W0 | `ckeys/w0` | this plan and the ADR amendment | landed, #854 |
| W1 | `ckeys/w1` | container identity: `container.id` from `\|c:`, the tracer header and payload field, and stats `ContainerID`; `container.image.tags` from stats `ImageTag` and `docker_in`'s rename; carriers retired; `statsd_out`, `datadog_out`, and `datadog_trace_out` restore | landed, #856 |
| W2 | `ckeys/w2` | syslog PROCID and `tag[pid]` to `process.pid`; `syslog_out` reads it back | landed, #855 |
| W3 | `ckeys/w3` | carry leftovers: the uncounted `\|h:` sanitize at `statsd_out`; `classes.rs` `host_is_omitted` on a mixed `host` array; `is_datadog_stats`' per-event interner lookup | landed, #853 |
| W4a | `ckeys/w4a` | service identity on tags: DogStatsD and Datadog series, sketches, events, and checks `env:`, `version:`, `service:` to the keys; `statsd_out` and `datadog_out` write them back as tags | landed, #859 |
| W4b | `ckeys/w4b` | service identity on Datadog logs: `service` to `service.name`; `ddtags` `env:`, `version:`, `service:` lifted; `datadog_out` writes them back | landed, #858 |
| W4c | `ckeys/w4c` | service identity on traces and APM stats: span meta `env` and `version`, `TracerPayload` `env` and `app_version`, stats `Env`, `Version`, and `Service` to the keys; carriers retired; both trace sinks restore | landed, #857 |

W1, W2, and W3 branched off `main`, and W4a, W4b, and W4c off `ckeys/w1`, because they edit the
Datadog codec's tables and sink paths that W1 changed first. All of them land as one linear
`gh stack`, #860, in the order W0, W3, W2, W1, W4a, W4b, W4c.

## Mechanics

Each code PR followed the shape the `carry` stream set for `host.name`:

- The decoder writes the key, the carrier or bare attribute is gone in the same PR (no alias, no
  dual write), and each sink in the family writes it back in every record kind's native form.
- Fixed-point and interop tests show the carrier appears nowhere and each relay stays byte-stable
  modulo its permitted normalizations (`crates/logit-proto/tests/datadog_*`, `statsd_*`,
  `syslog_*`, and the corpora under `testdata/interop/`). A new normalization joins the codec's
  module-doc list.
- `script/transit gen` regenerates the transit spec rows and `classes.rs` spellings wherever the
  change touches a spec'd source or sink.
- [ADR `process-hostname`](../adr/process-hostname.md)'s inventory row for the key names its new
  writers, and the family's relay ADR gets a dated amendment citing the Agent or exporter source
  (datadog-agent 7.83.3, file and function) for the mapping.
- `docs/design/data-model.md`, `docs/deploying.md`, `docs/datadog.md`, `docs/known-gaps/`, and
  `AGENTS.md` change wherever they describe the old spelling. Grep the repository for the old
  attribute name and the carrier constant's string before opening the PR.
- Each regression test is run once with its fix reverted, and the PR body says so per test.

Review of the `carry` stream caught the same patterns in most PRs; check each before pushing:

- A list checked one row at a time: a codec's normalizations, an ADR's decode or per-sink table.
- A `*.normalized`, `*.omitted`, or `*.dropped` count that fires for a tag that never reached the
  wire.
- An event-wins merge that shadows a resource key, uncounted, on a form with no payload field.
- A stale config field doc or schema (`script/schema` after any `logit-config` change).
