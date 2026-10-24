---
created: 2026-10-24
updated: 2026-10-24
---

# Enabling plan: the remaining common keys — service, deployment, container, and process identity

## Context

Every input with a protocol host field now writes `host.name`, and no sink rebuilds a host from a
wire form. The same facts for service, deployment environment, version, container, and process
still reach the model under protocol spellings: a bare `env` tag, a `_logit.datadog.tracer.env`
carrier, a `_logit.statsd.container_id` carrier, a string-or-integer `syslog.pid`. A sink that wants
one of them has to know every family's spelling, and a cross-family relay (`datadog_in` to
`otlp_out`, say) delivers none of them under the name its destination reads.

This plan moves each of those facts onto its OpenTelemetry semantic-conventions key, input by
input, under stream key `ckeys`. W0 is this plan and the ADR amendment. Nothing below W0 has
landed.

Goals:

- Every input whose protocol defines one of the six keys in [The keys](#the-keys) writes it, at
  the scope where it is identity, and the carrier or bare attribute it replaces is gone.
- Every sink in that input's family writes the key back in its own wire form, so each like-pair
  relay stays a fixed point modulo its named normalizations.
- A Datadog-family sink writes each key under Datadog's spelling whatever source wrote it, so
  an OTLP sender's `deployment.environment.name` reaches Datadog as `env`.

Non-goals:

- Any key an input would have to infer from a field its protocol doesn't define. A Telegraf
  `env` tag on `influxdb_in`, a parsed log's `service` field on `tail_in`, or a carbon path
  segment stays what it is, and an operator's transform puts it on a common key.
- The Datadog fields listed in [What stays as it is](#what-stays-as-it-is).
- `otlp_out` and `splunk_hec_out` changes beyond what a test shows is needed. Both pass the keys
  through as attributes today.

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

### The keys

| Key | Type | Written from (protocol-defined fields only) |
|---|---|---|
| `service.name` | string | a Datadog log's `service`; a `service:` tag on a DogStatsD line and on a Datadog series, sketch, event, or service check; a stats payload's `Service`. Spans and APM stats groups already write it |
| `deployment.environment.name` | string | an `env:` tag (DogStatsD, Datadog metrics, events, and checks); a Datadog log's `ddtags` `env:`; a span's `env` meta; a `TracerPayload`'s `env`; a stats payload's `Env` |
| `service.version` | string | a `version:` tag; a `ddtags` `version:`; a span's `version` meta; a `TracerPayload`'s `app_version`; a stats payload's `Version` |
| `container.id` | string | DogStatsD `\|c:` on metrics, events, and checks; the `Datadog-Container-ID` header and a `TracerPayload`'s `container_id`; a stats payload's `ContainerID` |
| `container.image.tags` | array of string | a stats payload's `ImageTag`, as one element; `docker_in`'s image reference tag, renamed from `container.image.tag` |
| `process.pid` | integer | a syslog PROCID or RFC 3164 `tag[pid]` that is canonical decimal |

The names are semconv's current ones. `deployment.environment` and `container.image.tag` are
deprecated there, and pre-release `logit` keeps no alias for either. Each key joins
[data-model.md](../design/data-model.md)'s "Well-known attribute names" in the PR that first
writes it.

### Scope

A tag or field on one line, record, or span writes the key on that event. A field of a whole
payload (a `TracerPayload`, a `ClientStatsPayload`, the tracer's request headers) writes it on the
batch resource, because a batch is one payload. A sink reads the event's value, else the
resource's.

A sink that writes a per-payload field reads the resource only, and follows what `datadog_out`
does for a `TracerPayload`'s `hostname` today: the field is the resource's value, an event's own
value is an ordinary span tag, and in a form with no payload field (v0.4, v0.5) the resource value
rides as a span tag that a differing event value shadows, counted lost (`is_carrier` and
`DatadogEncoder::wire_span` in `crates/logit-proto/src/datadog/traces.rs`).

### Collisions

**At decode, the protocol's own form wins.** When a protocol's own field and a sender's tag name
the same fact (a span's `env` meta beside a `deployment.environment.name` meta key, `|c:` beside a
`container.id:` tag, or `env:` beside `deployment.environment.name:` in one tag list), the key takes
the protocol's value and the tag is dropped, counted
`logit.input.tags.omitted{reason="reserved_key"}` once per wire tag. That's the counter and reason
a `host.name:` tag beside `#host:` uses (`insert_host` in `crates/logit-proto/src/statsd/decode.rs`,
`insert_wire_host` in `crates/logit-proto/src/datadog/mod.rs`).

**At encode, the key wins.** A bare `env`, `version`, or `service` attribute beside its key can
only come from an operator's transform or a source in another family, and writing both would put
two values of one tag on the wire. The bare one is omitted, counted
`logit.output.tags.omitted`: `reason="unrepresentable"` in a tag list, as `statsd_out` counts a
valued `host` beside `host.name`, and `reason="reserved_key"` for a Datadog log's top-level key, as
`datadog_out` counts a log's `hostname` attribute.

### Datadog's spellings at a Datadog-family sink

`datadog_out`, `datadog_trace_out`, and `statsd_out` under `format: dogstatsd` write each key under
the name Datadog's OTLP mapping uses (datadog-agent 7.83.3,
`pkg/opentelemetry-mapping-go/otlp/attributes/attributes.go`, `coreMapping` and
`ContainerMappings`):

| Key | Metrics, events, checks | Logs | Traces and stats |
|---|---|---|---|
| `service.name` | `service:` tag | `service` field | the span's or group's `service`; a stats payload's `Service` |
| `deployment.environment.name` | `env:` tag | `env:` in `ddtags` | `env` meta; `TracerPayload.env`; stats `Env` |
| `service.version` | `version:` tag | `version:` in `ddtags` | `version` meta; `TracerPayload.app_version`; stats `Version` |
| `container.id` | `container_id:` tag at `datadog_out`; `\|c:` at `statsd_out` | `container_id:<id>` appended to `ddtags` | `TracerPayload.container_id` and the `Datadog-Container-ID` header; stats `ContainerID` |
| `container.image.tags` | the first element as an `image_tag:` tag | the first element as `image_tag:` in `ddtags` | the first element as stats `ImageTag` |

`otlp_out` and `splunk_hec_out` send the keys as attributes under their semconv names.

Datadog's `image_tag` holds one value, so a Datadog-family sink writes the first element of
`container.image.tags` and omits the rest, counted. On a log, `container.id` is appended to
`ddtags` as `container_id:<id>`, as the Agent tags a container's logs; a `container_id:` entry
already in the log's `ddtags` string loses to the key, removed and counted
`logit.output.tags.omitted{reason="reserved_key"}`, as a log's `hostname` beside `host.name` is.
W1 writes this and records it in the Datadog relay ADR.

### Datadog logs: `ddtags` is lifted, not split

A Datadog log's `ddtags` is one comma-joined string. W4b lifts its `env:`, `version:`, and
`service:` tags onto the three keys and leaves every other tag in the one `ddtags` string attribute,
unchanged. A log's own `service` field wins over a `ddtags` `service:`, and the losing one is
dropped, counted as a decode collision. `datadog_out` writes `env:` and `version:` back into
`ddtags` from the keys for any log, an OTLP log included, and writes `service.name` as the log's
`service` field.

A full split, every tag to its own attribute, was rejected. A tag and a top-level JSON field are
different things in Datadog, and `datadog_out` writes every attribute as a field. A split tag would come back from a relay as a
field, so the round trip would change what Datadog stores. The three lifted tags are the
exception because Unified Service Tagging defines them, and they have a typed home on every route.

### What stays as it is

- **`AgentPayload.env` and `agentVersion`, and a `StatsPayload`'s `agentEnv` and `agentVersion`**
  (`_logit.datadog.agent.env`, `_logit.datadog.agent.version`). They're the relaying Agent's
  identity, not the application's, as `_logit.datadog.agent.hostname` is.
- **A stats payload's `GitCommitSha`** (`_logit.datadog.stats.git_commit_sha`). semconv's `vcs.*`
  keys describe a repository change, not a deployed build, and nothing outside the Datadog family
  reads it.
- **Syslog APP-NAME** stays `syslog.tag`. It names a program (`sshd`, `nginx`), not a service, and
  writing it as `service.name` would give every daemon on a host its own service.
- **A tracer's `language_name`, `tracer_version`, and `runtime_id`** stay
  `_logit.datadog.tracer.*` carriers. They describe the tracing library, and only
  `datadog_trace_out` and `datadog_out` read them.
- **A Datadog log's `ddsource`** stays `ddsource`. It names the log's integration, and semconv has
  no key for it.
- **`influxdb_in`, `graphite_in`, `lines_in`, and `tail_in`** write none of the six keys. Their
  protocols define no such field, so a value they carry is an operator's to move with a transform.

## Per-input mapping

Each row is what `origin/main` decodes today, read from the code. "Today" names the attribute and
its scope; "Becomes" is the key it moves to, on the same scope unless the row says otherwise.

### Container identity (W1)

| Input | Wire field | Today | Becomes | Code |
|---|---|---|---|---|
| `statsd_in` | `\|c:` on a metric line, event, or service check | `_logit.statsd.container_id`, event, the raw field | `container.id`, event | `insert_container_id` in `crates/logit-proto/src/statsd/decode.rs` |
| `datadog_trace_in` | `Datadog-Container-ID` header | `_logit.datadog.tracer.container_id`, resource, filled only when the payload's own field is empty | `container.id`, resource | `apply_tracer_headers` and `apply_stats_headers` in `crates/logit-inputs/src/datadog_trace.rs`, through `TRACER_STR_HEADERS` in `crates/logit-proto/src/datadog/mod.rs` |
| `datadog_in` traces, `datadog_trace_in` `/v0.7/traces` | `TracerPayload.container_id` | `_logit.datadog.tracer.container_id`, resource | `container.id`, resource | `tracer_resource` in `crates/logit-proto/src/datadog/traces.rs` |
| `datadog_in` stats, `datadog_trace_in` `/v0.6/stats` | `ClientStatsPayload.ContainerID` | `_logit.datadog.tracer.container_id`, resource (the tracer carrier, shared with traces) | `container.id`, resource | `PAYLOAD_STRINGS` and `payload_string_mut` in `crates/logit-proto/src/datadog/stats.rs` |
| `datadog_in` stats, `datadog_trace_in` `/v0.6/stats` | `ClientStatsPayload.ImageTag` | `_logit.datadog.stats.image_tag`, resource | `container.image.tags`, a one-element array, resource | the same |
| `docker_in` | the image reference's tag in `config.v2.json` | `container.image.tag`, resource | `container.image.tags`, a one-element array, resource | `resource` in `crates/logit-inputs/src/docker.rs` |

`datadog_out` writes no container identity on any route today, so W1's `container_id:` and
`image_tag:` tags are new output there. `statsd_out` writes `|c:` from the carrier.

**`|c:` isn't always a container id.** The Agent reads `|c:` as origin-detection local data
(datadog-agent 7.83.3, `comp/core/tagger/origindetection/origindetection.go`, `ParseLocalData`):
a bare id or a `cid-` or `ci-` prefixed one is a container id, an `in-` prefixed one is a cgroup v2
inode, and a comma pair holds one of each. Today the decoder keeps the whole field verbatim. W1
will decode a bare or `ci-` prefixed value to `container.id`, with the prefix stripped, and an
`in-<inode>` to the carrier `_logit.statsd.cgroup_inode`, since no neutral key names a cgroup
inode; a comma pair yields both. `statsd_out` will write `|c:<id>` bare, appending `,in-<inode>`
when the carrier is present. A `ci-` prefix relaying as the bare id is a named normalization of
the DogStatsD pair: the bare form is the one every Agent version reads. W1 records this in the
DogStatsD relay ADR.

### Process identity (W2)

| Input | Wire field | Today | Becomes | Code |
|---|---|---|---|---|
| `syslog_in` | RFC 5424 PROCID | `syslog.pid`, event: `U64` when canonical decimal, else `Str` | `process.pid` (`U64`) when canonical decimal | `parse_5424` and `canonical_pid` in `crates/logit-proto/src/syslog/decode.rs` |
| `syslog_in` | RFC 3164 `tag[pid]` | the same | the same | `parse_3164` |

Canonical decimal is ASCII digits with no leading zero, or `0`. A PROCID that isn't (`-` aside,
which writes nothing) stays the string `syslog.pid`, since it isn't a process id and `syslog_out`
has to relay it unchanged. `syslog_out` reads both spellings back as PROCID (`resolve_pid` in
`crates/logit-outputs/src/syslog.rs`).

### Service identity on tags (W4a)

| Input | Wire field | Today | Becomes | Code |
|---|---|---|---|---|
| `statsd_in` | `env:`, `version:`, `service:` tags on a metric line, event, or service check | bare `env`, `version`, `service`, event | the three keys, event | `insert_tags` in `crates/logit-proto/src/statsd/decode.rs` |
| `datadog_in` | the same tags on a series (v1, v2 JSON, v2 protobuf), distribution point, or sketch | the same | the same | `insert_tags` in `crates/logit-proto/src/datadog/tags.rs`, called from `series.rs` |
| `datadog_in` | the same tags on an event or service check | the same | the same | the same, called from `events.rs` and `service_checks.rs` |

### Service identity on Datadog logs (W4b)

| Input | Wire field | Today | Becomes | Code |
|---|---|---|---|---|
| `datadog_in` logs | `service` | `service`, event | `service.name`, event | `ATTR_SERVICE` in `crates/logit-proto/src/datadog/logs.rs` |
| `datadog_in` logs | `ddtags` | one `Str` `ddtags`, event, never split | `env:`, `version:`, and `service:` lifted onto the keys; the rest stays `ddtags` | `ATTR_DDTAGS`, the same file |

`datadog_out` writes a log's `service` from `service` today, else from `service.name`
(`LogKeys` and the encoder's reserved-field loop in `logs.rs`).

### Service identity on traces and stats (W4c)

| Input | Wire field | Today | Becomes | Code |
|---|---|---|---|---|
| `datadog_in` traces, `datadog_trace_in` | a span's `env` and `version` meta | bare `env`, `version`, event (every `meta` key lands verbatim) | `deployment.environment.name`, `service.version`, event | `span_event` in `crates/logit-proto/src/datadog/traces.rs` |
| the same | `TracerPayload.env`, `.app_version` | `_logit.datadog.tracer.env`, `.app_version`, resource | `deployment.environment.name`, `service.version`, resource | `tracer_resource` in `traces.rs` |
| `datadog_in` stats, `datadog_trace_in` `/v0.6/stats` | `ClientStatsPayload.Env`, `.Version` | `_logit.datadog.tracer.env`, `.app_version`, resource (the tracer carriers) | the same two keys, resource | `payload_string_mut` in `stats.rs` |
| the same | `ClientStatsPayload.Service` | `_logit.datadog.stats.service`, resource | `service.name`, resource | the same |

A span's `service` and a stats group's `Service` are `service.name` on the event already
(`ATTR_SERVICE_NAME` in `crates/logit-proto/src/datadog/mod.rs`).

## Workstreams

| WS | Branch | Scope |
|---|---|---|
| W0 | `ckeys/w0` | this plan and the ADR amendment |
| W1 | `ckeys/w1` | container identity: `container.id` from `\|c:`, the tracer header and payload field, and stats `ContainerID`; `container.image.tags` from stats `ImageTag` and `docker_in`'s rename; carriers retired; `statsd_out`, `datadog_out`, and `datadog_trace_out` restore |
| W2 | `ckeys/w2` | syslog PROCID and `tag[pid]` to `process.pid`; `syslog_out` reads it back |
| W3 | `ckeys/w3` | carry leftovers: the uncounted `\|h:` sanitize at `statsd_out`; `classes.rs` `host_is_omitted` on a mixed `host` array; `is_datadog_stats`' per-event interner lookup |
| W4a | `ckeys/w4a` | service identity on tags: DogStatsD and Datadog series, sketches, events, and checks `env:`, `version:`, `service:` to the keys; `statsd_out` and `datadog_out` write them back as tags |
| W4b | `ckeys/w4b` | service identity on Datadog logs: `service` to `service.name`; `ddtags` `env:`, `version:`, `service:` lifted; `datadog_out` writes them back |
| W4c | `ckeys/w4c` | service identity on traces and APM stats: span meta `env` and `version`, `TracerPayload` `env` and `app_version`, stats `Env`, `Version`, and `Service` to the keys; carriers retired; both trace sinks restore |

W1, W2, and W3 branch off `main`. W4a, W4b, and W4c branch off `ckeys/w1`, because they
edit the Datadog codec's tables and sink paths that W1 changes first. All of them land as one `gh stack`.

## Mechanics

Each code PR follows the shape the `carry` stream set for `host.name`:

- The decoder writes the key, the carrier or bare attribute is gone in the same PR (no alias, no
  dual write), and each sink in the family writes it back in every record kind's native form.
- Fixed-point and interop tests show the carrier appears nowhere and each relay stays byte-stable
  modulo its permitted normalizations (`crates/logit-proto/tests/datadog_*`, `statsd_*`,
  `syslog_*`, and the corpora under `testdata/interop/`). A new normalization joins the codec's
  module-doc list.
- `script/transit gen` regenerates the transit spec rows and `classes.rs` spellings wherever the
  change touches a spec'd source or sink.
- [ADR `process-hostname`](../adr/process-hostname.md)'s inventory row for the key changes from
  future to present tense, and the family's relay ADR gets a dated amendment citing the Agent or
  exporter source (datadog-agent 7.83.3, file and function) for the mapping.
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
